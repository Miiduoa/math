const connectionString = process.env.DATABASE_URL || '';
let pool = null;
let Pool = null; // lazy-loaded from 'pg'

export function isDbEnabled(){
  return Boolean(connectionString);
}

export async function getPool(){
  if(!isDbEnabled()) throw new Error('DATABASE_URL not configured');
  if(!pool){
    if(!Pool){ const mod = await import('pg'); Pool = mod.Pool; }
    pool = new Pool({ connectionString, ssl: getSslConfig() });
    await initSchema(pool);
  }
  return pool;
}

function getSslConfig(){
  // Render PostgreSQL 通常需要 SSL；允許自簽
  const disable = String(process.env.PGSSL || '').toLowerCase() === 'disable';
  if(disable) return false;
  return { rejectUnauthorized: false };
}

async function initSchema(pool){
  await pool.query(`
  create table if not exists categories (
    id text primary key,
    name text not null
  );
  -- global default settings (legacy; kept for compatibility)
  create table if not exists settings (
    key text primary key,
    data jsonb not null
  );
  -- per-user settings
  create table if not exists user_settings (
    user_id text primary key,
    data jsonb not null
  );
  -- transactions with optional user_id (indexed)
  create table if not exists transactions (
    id text primary key,
    user_id text,
    date text not null,
    type text not null,
    category_id text not null references categories(id) on delete restrict,
    currency text not null default 'TWD',
    rate numeric not null default 1,
    amount numeric not null,
    claim_amount numeric default 0,
    claimed boolean default false,
    emotion text default '',
    motivation text default '',
    note text default ''
  );
  create index if not exists idx_transactions_user on transactions(user_id);
  -- global model (legacy)
  create table if not exists model_words (
    word text primary key,
    counts jsonb not null default '{}'::jsonb
  );
  -- per-user model
  create table if not exists user_model_words (
    user_id text not null,
    word text not null,
    counts jsonb not null default '{}'::jsonb,
    primary key(user_id, word)
  );
  -- link tables for LINE bot <-> web session
  create table if not exists user_links (
    line_user_id text primary key,
    web_user_id text not null,
    created_at timestamptz not null default now()
  );
  create table if not exists link_codes (
    code text primary key,
    line_user_id text not null,
    expires_at timestamptz not null
  );
  -- notes table
  create table if not exists notes (
    id text primary key,
    user_id text,
    title text default '',
    content text not null,
    tags text[] default '{}',
    emoji text default '',
    color text default '',
    pinned boolean default false,
    archived boolean default false,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
  );
  create index if not exists idx_notes_user on notes(user_id);
  -- reminders table
  create table if not exists reminders (
    id text primary key,
    user_id text,
    title text not null,
    due_at timestamptz,
    repeat text not null default 'none',
    weekdays integer[] default '{}',
    month_day integer,
    priority text not null default 'medium',
    tags text[] default '{}',
    note text default '',
    done boolean default false,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
  );
  create index if not exists idx_reminders_user on reminders(user_id);
  `);
  // seed settings and default categories
  const res = await pool.query('select count(*)::int as c from categories');
  if((res.rows?.[0]?.c||0) === 0){
    await pool.query(
      `insert into categories(id, name) values ($1,$2),($3,$4),($5,$6),($7,$8) on conflict do nothing`,
      ['food','餐飲','transport','交通','shopping','購物','salary','薪資']
    );
  }
  await pool.query(
    `insert into settings(key, data) values ($1, $2)
     on conflict (key) do nothing`,
    ['app', { key:'app', baseCurrency:'TWD', monthlyBudgetTWD:0, savingsGoalTWD:0, nudges:true, appearance:'system', categoryBudgets:{} }]
  );
}

export const db = {
  async upsertLink(lineUserId, webUserId){
    const p = await getPool();
    await p.query('insert into user_links(line_user_id, web_user_id) values($1,$2) on conflict (line_user_id) do update set web_user_id=excluded.web_user_id', [lineUserId, webUserId]);
    return true;
  },
  async getLinkedWebUser(lineUserId){
    const p = await getPool();
    const r = await p.query('select web_user_id from user_links where line_user_id=$1', [lineUserId]);
    return r.rows[0]?.web_user_id || null;
  },
  async listAllLineUserIds(){
    const p = await getPool();
    const r = await p.query('select line_user_id from user_links');
    return r.rows.map(x=> x.line_user_id).filter(Boolean);
  },
  async createLinkCode(lineUserId, ttlSeconds=300){
    const p = await getPool();
    const code = (Math.random().toString(36).slice(2,8)+Math.random().toString(36).slice(2,8)).slice(0,10);
    const expires = new Date(Date.now()+ttlSeconds*1000).toISOString();
    await p.query('insert into link_codes(code,line_user_id,expires_at) values($1,$2,$3) on conflict do nothing', [code, lineUserId, expires]);
    return code;
  },
  async consumeLinkCode(code){
    const p = await getPool();
    const r = await p.query('delete from link_codes where code=$1 and expires_at>now() returning line_user_id', [code]);
    return r.rows[0]?.line_user_id || null;
  },
  async getCategories(){
    const p = await getPool();
    const r = await p.query('select id, name from categories order by name');
    return r.rows;
  },
  async addCategory(name){
    const id = String(name).trim().toLowerCase().replace(/\s+/g,'-');
    if(!id) return null;
    const p = await getPool();
    await p.query('insert into categories(id,name) values($1,$2) on conflict do nothing', [id, String(name).trim()]);
    const r = await p.query('select id, name from categories where id=$1', [id]);
    return r.rows[0] || null;
  },
  async deleteCategory(id){
    const p = await getPool();
    // forbid delete if used
    const used = await p.query('select 1 from transactions where category_id=$1 limit 1', [id]);
    if(used.rowCount>0) return false;
    await p.query('delete from categories where id=$1', [id]);
    return true;
  },
  async getSettings(userId){
    const p = await getPool();
    if(userId){
      const r = await p.query('select data from user_settings where user_id=$1', [userId]);
      return r.rows[0]?.data || { key:'app', baseCurrency:'TWD', monthlyBudgetTWD:0, savingsGoalTWD:0, nudges:true, appearance:'system', categoryBudgets:{} };
    }else{
      const r = await p.query('select data from settings where key=$1', ['app']);
      return r.rows[0]?.data || { key:'app', baseCurrency:'TWD', monthlyBudgetTWD:0, savingsGoalTWD:0, nudges:true, appearance:'system', categoryBudgets:{} };
    }
  },
  async setSettings(userId, patch){
    const p = await getPool();
    const cur = await this.getSettings(userId);
    const next = { ...cur, ...patch, key:'app' };
    if(userId){
      await p.query('insert into user_settings(user_id,data) values($1,$2) on conflict (user_id) do update set data=excluded.data', [userId, next]);
    }else{
      await p.query('insert into settings(key,data) values($1,$2) on conflict (key) do update set data=excluded.data', ['app', next]);
    }
    return next;
  },
  async getTransactions(userId){
    const p = await getPool();
    const r = await p.query('select * from transactions where user_id=$1 order by date desc, id desc', [userId||null]);
    return r.rows.map(row=>({
      id: row.id,
      date: row.date,
      type: row.type,
      categoryId: row.category_id,
      currency: row.currency,
      rate: Number(row.rate),
      amount: Number(row.amount),
      claimAmount: Number(row.claim_amount||0),
      claimed: row.claimed===true,
      emotion: row.emotion||'',
      motivation: row.motivation||'',
      note: row.note||''
    }));
  },
  async addTransaction(userId, payload){
    const p = await getPool();
    const id = (globalThis.crypto?.randomUUID && globalThis.crypto.randomUUID()) || String(Date.now())+Math.random().toString(16).slice(2);
    await p.query(`insert into transactions(
      id, user_id, date, type, category_id, currency, rate, amount, claim_amount, claimed, emotion, motivation, note
    ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, [
      id, userId||null, payload.date, payload.type, payload.categoryId, payload.currency||'TWD', Number(payload.rate)||1,
      Number(payload.amount)||0, Number(payload.claimAmount)||0, payload.claimed===true, payload.emotion||'', payload.motivation||'', payload.note||''
    ]);
    return { id, ...payload };
  },
  async updateTransaction(userId, id, patch){
    const p = await getPool();
    // fetch current
    const cur = await this.getTransactionById(userId, id);
    if(!cur) return null;
    const next = { ...cur, ...patch };
    await p.query(`update transactions set date=$3,type=$4,category_id=$5,currency=$6,rate=$7,amount=$8,claim_amount=$9,claimed=$10,emotion=$11,motivation=$12,note=$13 where id=$1 and user_id=$2`, [
      id, userId||null, next.date, next.type, next.categoryId, next.currency||'TWD', Number(next.rate)||1, Number(next.amount)||0, Number(next.claimAmount)||0, next.claimed===true, next.emotion||'', next.motivation||'', next.note||''
    ]);
    return next;
  },
  async getTransactionById(userId, id){
    const p = await getPool();
    const r = await p.query('select * from transactions where id=$1 and user_id=$2', [id, userId||null]);
    const row = r.rows[0];
    if(!row) return null;
    return {
      id: row.id,
      date: row.date,
      type: row.type,
      categoryId: row.category_id,
      currency: row.currency,
      rate: Number(row.rate),
      amount: Number(row.amount),
      claimAmount: Number(row.claim_amount||0),
      claimed: row.claimed===true,
      emotion: row.emotion||'',
      motivation: row.motivation||'',
      note: row.note||''
    };
  },
  async deleteTransaction(userId, id){
    const p = await getPool();
    await p.query('delete from transactions where id=$1 and user_id=$2', [id, userId||null]);
    return true;
  },
  async exportAll(userId){
    const [categories, transactions, settings, model] = await Promise.all([
      this.getCategories(), this.getTransactions(userId), this.getSettings(userId), this._getAllModel(userId)
    ]);
    return { categories, transactions, settings, model };
  },
  async importAll(userId, data){
    if(!data || !Array.isArray(data.categories) || !Array.isArray(data.transactions)) return false;
    const p = await getPool();
    const client = await p.connect();
    try{
      await client.query('begin');
      // categories remain global (no delete)
      await client.query('delete from transactions where user_id=$1', [userId||null]);
      await client.query('delete from user_model_words where user_id=$1', [userId||null]);
      await client.query('delete from user_settings where user_id=$1', [userId||null]);
      for(const c of data.categories){ await client.query('insert into categories(id,name) values($1,$2) on conflict do nothing',[c.id,c.name]); }
      for(const t of data.transactions){
        await client.query(`insert into transactions(id,user_id,date,type,category_id,currency,rate,amount,claim_amount,claimed,emotion,motivation,note)
          values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) on conflict do nothing`,[
          t.id, userId||null, t.date, t.type, t.categoryId, t.currency||'TWD', Number(t.rate)||1, Number(t.amount)||0, Number(t.claimAmount)||0, t.claimed===true, t.emotion||'', t.motivation||'', t.note||''
        ]);
      }
      if(data.settings){
        await client.query('insert into user_settings(user_id,data) values($1,$2) on conflict (user_id) do update set data=excluded.data',[ userId||null, { key:'app', ...data.settings } ]);
      }
      if(Array.isArray(data.model)){
        for(const rec of data.model){
          if(rec && rec.word){ await client.query('insert into user_model_words(user_id,word,counts) values($1,$2,$3) on conflict (user_id,word) do update set counts=excluded.counts',[ userId||null, rec.word, rec.counts||{} ]); }
        }
      }
      await client.query('commit');
      return true;
    }catch(err){
      await client.query('rollback');
      throw err;
    }finally{
      client.release();
    }
  },
  async updateCategoryModelFromNote(userId, note, categoryId){
    if(!note || !categoryId) return false;
    const p = await getPool();
    const words = String(note).toLowerCase().split(/[^\p{L}\p{N}]+/u).filter(Boolean);
    for(const w of words){
      const r = await p.query('select counts from user_model_words where user_id=$1 and word=$2', [userId||null, w]);
      const rec = r.rows[0] || { counts:{} };
      rec.counts[categoryId] = (Number(rec.counts[categoryId])||0) + 1;
      await p.query('insert into user_model_words(user_id,word,counts) values($1,$2,$3) on conflict (user_id,word) do update set counts=excluded.counts', [userId||null, w, rec.counts]);
    }
    return true;
  },
  async suggestCategoryFromNote(userId, note){
    if(!note) return null;
    const p = await getPool();
    const words = String(note).toLowerCase().split(/[^\p{L}\p{N}]+/u).filter(Boolean);
    const scores = {};
    for(const w of words){
      const r = await p.query('select counts from user_model_words where user_id=$1 and word=$2', [userId||null, w]);
      const rec = r.rows[0];
      if(rec && rec.counts){
        for(const [k,v] of Object.entries(rec.counts)){
          scores[k] = (scores[k]||0) + Number(v||0);
        }
      }
    }
    let best=null,bestScore=0; for(const [k,v] of Object.entries(scores)){ if(v>bestScore){ bestScore=v; best=k; } }
    return best;
  },
  async _getAllModel(userId){
    const p = await getPool();
    const r = await p.query('select word, counts from user_model_words where user_id=$1', [userId||null]);
    return r.rows.map(x=> ({ word:x.word, counts:x.counts||{} }));
  },
  // Reminders CRUD
  async getReminders(userId){
    const p = await getPool();
    const r = await p.query('select * from reminders where user_id=$1 order by created_at desc', [userId||null]);
    return r.rows.map(row=>({
      id: row.id,
      title: row.title,
      dueAt: row.due_at ? row.due_at.toISOString() : null,
      repeat: row.repeat,
      weekdays: row.weekdays || [],
      monthDay: row.month_day,
      priority: row.priority,
      tags: row.tags || [],
      note: row.note || '',
      done: row.done === true,
      createdAt: row.created_at.toISOString(),
      updatedAt: row.updated_at.toISOString()
    }));
  },
  async addReminder(userId, payload){
    const p = await getPool();
    const id = (globalThis.crypto?.randomUUID && globalThis.crypto.randomUUID()) || String(Date.now())+Math.random().toString(16).slice(2);
    const now = new Date().toISOString();
    await p.query(`insert into reminders(
      id, user_id, title, due_at, repeat, weekdays, month_day, priority, tags, note, done, created_at, updated_at
    ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, [
      id, userId||null, payload.title, payload.dueAt || null, payload.repeat || 'none',
      payload.weekdays || [], payload.monthDay || null, payload.priority || 'medium',
      payload.tags || [], payload.note || '', payload.done === true, now, now
    ]);
    return { id, ...payload, createdAt: now, updatedAt: now };
  },
  async updateReminder(userId, id, patch){
    const p = await getPool();
    // fetch current
    const cur = await this.getReminderById(userId, id);
    if(!cur) return null;
    const next = { ...cur, ...patch, updatedAt: new Date().toISOString() };
    await p.query(`update reminders set 
      title=$3, due_at=$4, repeat=$5, weekdays=$6, month_day=$7, priority=$8, tags=$9, note=$10, done=$11, updated_at=$12
      where id=$1 and user_id=$2`, [
      id, userId||null, next.title, next.dueAt || null, next.repeat || 'none',
      next.weekdays || [], next.monthDay || null, next.priority || 'medium',
      next.tags || [], next.note || '', next.done === true, next.updatedAt
    ]);
    return next;
  },
  async getReminderById(userId, id){
    const p = await getPool();
    const r = await p.query('select * from reminders where id=$1 and user_id=$2', [id, userId||null]);
    const row = r.rows[0];
    if(!row) return null;
    return {
      id: row.id,
      title: row.title,
      dueAt: row.due_at ? row.due_at.toISOString() : null,
      repeat: row.repeat,
      weekdays: row.weekdays || [],
      monthDay: row.month_day,
      priority: row.priority,
      tags: row.tags || [],
      note: row.note || '',
      done: row.done === true,
      createdAt: row.created_at.toISOString(),
      updatedAt: row.updated_at.toISOString()
    };
  },
  async deleteReminder(userId, id){
    const p = await getPool();
    await p.query('delete from reminders where id=$1 and user_id=$2', [id, userId||null]);
    return true;
  },
  // Notes CRUD
  async getNotes(userId){
    const p = await getPool();
    const r = await p.query('select * from notes where user_id=$1 order by updated_at desc', [userId||null]);
    return r.rows.map(row=>({
      id: row.id,
      title: row.title || '',
      content: row.content,
      tags: row.tags || [],
      emoji: row.emoji || '',
      color: row.color || '',
      pinned: row.pinned === true,
      archived: row.archived === true,
      createdAt: row.created_at.toISOString(),
      updatedAt: row.updated_at.toISOString()
    }));
  },
  async addNote(userId, payload){
    const p = await getPool();
    const id = (globalThis.crypto?.randomUUID && globalThis.crypto.randomUUID()) || String(Date.now())+Math.random().toString(16).slice(2);
    const now = new Date().toISOString();
    await p.query(`insert into notes(
      id, user_id, title, content, tags, emoji, color, pinned, archived, created_at, updated_at
    ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, [
      id, userId||null, payload.title || '', payload.content, payload.tags || [],
      payload.emoji || '', payload.color || '', payload.pinned === true, payload.archived === true, now, now
    ]);
    return { id, ...payload, createdAt: now, updatedAt: now };
  },
  async updateNote(userId, id, patch){
    const p = await getPool();
    // fetch current
    const cur = await this.getNoteById(userId, id);
    if(!cur) return null;
    const next = { ...cur, ...patch, updatedAt: new Date().toISOString() };
    await p.query(`update notes set 
      title=$3, content=$4, tags=$5, emoji=$6, color=$7, pinned=$8, archived=$9, updated_at=$10
      where id=$1 and user_id=$2`, [
      id, userId||null, next.title || '', next.content, next.tags || [],
      next.emoji || '', next.color || '', next.pinned === true, next.archived === true, next.updatedAt
    ]);
    return next;
  },
  async getNoteById(userId, id){
    const p = await getPool();
    const r = await p.query('select * from notes where id=$1 and user_id=$2', [id, userId||null]);
    const row = r.rows[0];
    if(!row) return null;
    return {
      id: row.id,
      title: row.title || '',
      content: row.content,
      tags: row.tags || [],
      emoji: row.emoji || '',
      color: row.color || '',
      pinned: row.pinned === true,
      archived: row.archived === true,
      createdAt: row.created_at.toISOString(),
      updatedAt: row.updated_at.toISOString()
    };
  },
  async deleteNote(userId, id){
    const p = await getPool();
    await p.query('delete from notes where id=$1 and user_id=$2', [id, userId||null]);
    return true;
  }
};


