import { Pool } from 'pg';

const connectionString = process.env.DATABASE_URL || '';
let pool = null;

export function isDbEnabled(){
  return Boolean(connectionString);
}

export async function getPool(){
  if(!isDbEnabled()) throw new Error('DATABASE_URL not configured');
  if(!pool){
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
  create table if not exists settings (
    key text primary key,
    data jsonb not null
  );
  create table if not exists transactions (
    id text primary key,
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
  create table if not exists model_words (
    word text primary key,
    counts jsonb not null default '{}'::jsonb
  );
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
  async getSettings(){
    const p = await getPool();
    const r = await p.query('select data from settings where key=$1', ['app']);
    return r.rows[0]?.data || { key:'app', baseCurrency:'TWD', monthlyBudgetTWD:0, savingsGoalTWD:0, nudges:true, appearance:'system', categoryBudgets:{} };
  },
  async setSettings(patch){
    const p = await getPool();
    const cur = await this.getSettings();
    const next = { ...cur, ...patch, key:'app' };
    await p.query('insert into settings(key,data) values($1,$2) on conflict (key) do update set data=excluded.data', ['app', next]);
    return next;
  },
  async getTransactions(){
    const p = await getPool();
    const r = await p.query('select * from transactions order by date desc, id desc');
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
  async addTransaction(payload){
    const p = await getPool();
    const id = (globalThis.crypto?.randomUUID && globalThis.crypto.randomUUID()) || String(Date.now())+Math.random().toString(16).slice(2);
    await p.query(`insert into transactions(
      id, date, type, category_id, currency, rate, amount, claim_amount, claimed, emotion, motivation, note
    ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, [
      id, payload.date, payload.type, payload.categoryId, payload.currency||'TWD', Number(payload.rate)||1,
      Number(payload.amount)||0, Number(payload.claimAmount)||0, payload.claimed===true, payload.emotion||'', payload.motivation||'', payload.note||''
    ]);
    return { id, ...payload };
  },
  async updateTransaction(id, patch){
    const p = await getPool();
    // fetch current
    const cur = await this.getTransactionById(id);
    if(!cur) return null;
    const next = { ...cur, ...patch };
    await p.query(`update transactions set date=$2,type=$3,category_id=$4,currency=$5,rate=$6,amount=$7,claim_amount=$8,claimed=$9,emotion=$10,motivation=$11,note=$12 where id=$1`, [
      id, next.date, next.type, next.categoryId, next.currency||'TWD', Number(next.rate)||1, Number(next.amount)||0, Number(next.claimAmount)||0, next.claimed===true, next.emotion||'', next.motivation||'', next.note||''
    ]);
    return next;
  },
  async getTransactionById(id){
    const p = await getPool();
    const r = await p.query('select * from transactions where id=$1', [id]);
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
  async deleteTransaction(id){
    const p = await getPool();
    await p.query('delete from transactions where id=$1', [id]);
    return true;
  },
  async exportAll(){
    const [categories, transactions, settings, model] = await Promise.all([
      this.getCategories(), this.getTransactions(), this.getSettings(), this._getAllModel()
    ]);
    return { categories, transactions, settings, model };
  },
  async importAll(data){
    if(!data || !Array.isArray(data.categories) || !Array.isArray(data.transactions)) return false;
    const p = await getPool();
    const client = await p.connect();
    try{
      await client.query('begin');
      await client.query('delete from categories');
      await client.query('delete from transactions');
      await client.query('delete from model_words');
      await client.query('delete from settings where key=$1', ['app']);
      for(const c of data.categories){
        await client.query('insert into categories(id,name) values($1,$2) on conflict do nothing',[c.id,c.name]);
      }
      for(const t of data.transactions){
        await client.query(`insert into transactions(id,date,type,category_id,currency,rate,amount,claim_amount,claimed,emotion,motivation,note)
          values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) on conflict do nothing`,[
          t.id, t.date, t.type, t.categoryId, t.currency||'TWD', Number(t.rate)||1, Number(t.amount)||0, Number(t.claimAmount)||0, t.claimed===true, t.emotion||'', t.motivation||'', t.note||''
        ]);
      }
      if(data.settings){
        await client.query('insert into settings(key,data) values($1,$2) on conflict (key) do update set data=excluded.data',[ 'app', { key:'app', ...data.settings } ]);
      }
      if(Array.isArray(data.model)){
        for(const rec of data.model){
          if(rec && rec.word){ await client.query('insert into model_words(word,counts) values($1,$2) on conflict (word) do update set counts=excluded.counts',[ rec.word, rec.counts||{} ]); }
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
  async updateCategoryModelFromNote(note, categoryId){
    if(!note || !categoryId) return false;
    const p = await getPool();
    const words = String(note).toLowerCase().split(/[^\p{L}\p{N}]+/u).filter(Boolean);
    for(const w of words){
      const r = await p.query('select counts from model_words where word=$1', [w]);
      const rec = r.rows[0] || { counts:{} };
      rec.counts[categoryId] = (Number(rec.counts[categoryId])||0) + 1;
      await p.query('insert into model_words(word,counts) values($1,$2) on conflict (word) do update set counts=excluded.counts', [w, rec.counts]);
    }
    return true;
  },
  async suggestCategoryFromNote(note){
    if(!note) return null;
    const p = await getPool();
    const words = String(note).toLowerCase().split(/[^\p{L}\p{N}]+/u).filter(Boolean);
    const scores = {};
    for(const w of words){
      const r = await p.query('select counts from model_words where word=$1', [w]);
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
  async _getAllModel(){
    const p = await getPool();
    const r = await p.query('select word, counts from model_words');
    return r.rows.map(x=> ({ word:x.word, counts:x.counts||{} }));
  }
};


