import http from 'node:http';
import https from 'node:https';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { isDbEnabled, db as pgdb } from './db.js';

// Load local env variables from .env.local (if exists, non-production convenience)
try{
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const envPath = path.join(__dirname, '.env.local');
  if(fs.existsSync(envPath)){
    const content = fs.readFileSync(envPath, 'utf-8');
    content.split(/\r?\n/).forEach((line)=>{
      const m = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$/);
      if(!m) return;
      const key = m[1];
      let val = m[2];
      if((val.startsWith('"') && val.endsWith('"')) || (val.startsWith('\'') && val.endsWith('\''))){
        val = val.slice(1,-1);
      }
      if(!process.env[key]) process.env[key] = val;
    });
  }
}catch(_){ /* ignore */ }

const PORT = Number(process.env.PORT || 8787);
const LINE_CHANNEL_SECRET = process.env.LINE_CHANNEL_SECRET || '';
const LINE_CHANNEL_ACCESS_TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN || '';
const SESSION_SECRET = process.env.SESSION_SECRET || '';
const REQUIRE_AUTH = String(process.env.REQUIRE_AUTH||'false').toLowerCase()==='true';
const LINE_LOGIN_CHANNEL_ID = process.env.LINE_LOGIN_CHANNEL_ID || '';
const LINE_LOGIN_CHANNEL_SECRET = process.env.LINE_LOGIN_CHANNEL_SECRET || '';
const LINE_LOGIN_REDIRECT_URI = process.env.LINE_LOGIN_REDIRECT_URI || '';
const ADMIN_LINE_USER_ID = process.env.ADMIN_LINE_USER_ID || 'U5c7738d89a59ff402fd6b56f5472d351';
// Local model trainer scheduling
const MODEL_TRAIN_INTERVAL_MS = Number(process.env.MODEL_TRAIN_INTERVAL_MS || 300000); // default 5 min
const MODEL_TRAIN_ON_START = String(process.env.MODEL_TRAIN_ON_START||'true').toLowerCase()==='true';
const MODEL_TRAIN_USE_AI = String(process.env.MODEL_TRAIN_USE_AI||'true').toLowerCase()==='true';
const MODEL_TRAIN_AI_MAX_PER_CYCLE = Number(process.env.MODEL_TRAIN_AI_MAX_PER_CYCLE || 20);
const MODEL_TRAIN_RECENT_DAYS = Number(process.env.MODEL_TRAIN_RECENT_DAYS || 7);

// In-memory store (demo only)
const store = {
  categories: [],
  transactions: [],
  settings: { key: 'app', baseCurrency: 'TWD', monthlyBudgetTWD: 0, savingsGoalTWD: 0, nudges: true, categoryBudgets: {} },
  model: [] // [{ word, counts: { catId: number } }]
};

// File-based per-user store (when DATABASE_URL is not set)
function ensureDir(p){ try{ fs.mkdirSync(p, { recursive:true }); }catch(_){ } }
function readJson(pathname, fallback){ try{ return JSON.parse(fs.readFileSync(pathname,'utf-8')); }catch(_){ return fallback; } }
function writeJson(pathname, data){ try{ fs.writeFileSync(pathname, JSON.stringify(data,null,2)); return true; }catch(_){ return false; } }
const DATA_BASE = process.env.DATA_DIR || path.resolve('.', 'data', 'users');
function userDirFor(userId){
  const base = DATA_BASE;
  const safe = String(userId||'anonymous').replace(/[^a-zA-Z0-9:_.-]/g,'_');
  const dir = path.join(base, safe);
  ensureDir(dir);
  return dir;
}
const fileStore = {
  migrateUserData(oldId, newId){
    if(!oldId || !newId || oldId===newId) return false;
    const oldDir = userDirFor(oldId);
    const newDir = userDirFor(newId);
    try{
      // if new has no transactions but old has, copy over
      const oldTx = readJson(path.join(oldDir,'transactions.json'), []);
      const newTx = readJson(path.join(newDir,'transactions.json'), []);
      if(Array.isArray(oldTx) && oldTx.length>0 && Array.isArray(newTx) && newTx.length===0){
        // copy categories/settings/model if present
        const cats = readJson(path.join(oldDir,'categories.json'), null); if(cats) writeJson(path.join(newDir,'categories.json'), cats);
        const set = readJson(path.join(oldDir,'settings.json'), null); if(set) writeJson(path.join(newDir,'settings.json'), set);
        const model = readJson(path.join(oldDir,'model.json'), null); if(model) writeJson(path.join(newDir,'model.json'), model);
        writeJson(path.join(newDir,'transactions.json'), oldTx);
        return true;
      }
    }catch(_){ }
    return false;
  },
  seedIfNeeded(userId){
    const dir = userDirFor(userId);
    const catsPath = path.join(dir, 'categories.json');
    if(!fs.existsSync(catsPath)){
      writeJson(catsPath, [ { id:'food', name:'餐飲' }, { id:'transport', name:'交通' }, { id:'shopping', name:'購物' }, { id:'salary', name:'薪資' } ]);
    }
    const setPath = path.join(dir, 'settings.json');
    if(!fs.existsSync(setPath)){
      writeJson(setPath, { key:'app', baseCurrency:'TWD', monthlyBudgetTWD:0, savingsGoalTWD:0, nudges:true, appearance:'system', categoryBudgets:{} });
    }
    const txPath = path.join(dir, 'transactions.json'); if(!fs.existsSync(txPath)) writeJson(txPath, []);
    const modelPath = path.join(dir, 'model.json'); if(!fs.existsSync(modelPath)) writeJson(modelPath, []);
  },
  getCategories(userId){ this.seedIfNeeded(userId); return readJson(path.join(userDirFor(userId),'categories.json'), []); },
  addCategory(userId, name){
    const arr = this.getCategories(userId);
    const id = String(name).trim().toLowerCase().replace(/\s+/g,'-'); if(!id) return null;
    if(arr.some(c=>c.id===id)) return arr.find(c=>c.id===id);
    const rec = { id, name:String(name).trim() }; arr.push(rec);
    writeJson(path.join(userDirFor(userId),'categories.json'), arr);
    return rec;
  },
  deleteCategory(userId, id){
    const txs = this.getTransactions(userId);
    if(txs.some(t=>t.categoryId===id)) return false;
    const arr = this.getCategories(userId).filter(c=>c.id!==id);
    writeJson(path.join(userDirFor(userId),'categories.json'), arr);
    return true;
  },
  getSettings(userId){ this.seedIfNeeded(userId); return readJson(path.join(userDirFor(userId),'settings.json'), { key:'app' }); },
  setSettings(userId, patch){ const cur=this.getSettings(userId); const next={ ...cur, ...patch, key:'app' }; writeJson(path.join(userDirFor(userId),'settings.json'), next); return next; },
  getTransactions(userId){ this.seedIfNeeded(userId); const rows=readJson(path.join(userDirFor(userId),'transactions.json'), []); return rows.sort((a,b)=> (b.date||'').localeCompare(a.date||'')); },
  addTransaction(userId, payload){ const rows=this.getTransactions(userId); const id=(crypto.randomUUID&&crypto.randomUUID())||String(Date.now())+Math.random().toString(16).slice(2); const rec={ id, ...payload }; rows.unshift(rec); writeJson(path.join(userDirFor(userId),'transactions.json'), rows); return rec; },
  getTransactionById(userId, id){ return this.getTransactions(userId).find(t=>t.id===id)||null; },
  updateTransaction(userId, id, patch){ const rows=this.getTransactions(userId); const idx=rows.findIndex(t=>t.id===id); if(idx<0) return null; rows[idx]={ ...rows[idx], ...patch }; writeJson(path.join(userDirFor(userId),'transactions.json'), rows); return rows[idx]; },
  deleteTransaction(userId, id){
    let rows=this.getTransactions(userId);
    const idx = rows.findIndex(t=> t.id===id);
    if(idx>=0){
      rows.splice(idx,1); // remove only the first matched record to avoid deleting duplicates
      writeJson(path.join(userDirFor(userId),'transactions.json'), rows);
      return true;
    }
    return false;
  },
  updateCategoryModelFromNote(userId, note, categoryId){ if(!note||!categoryId) return false; const model=readJson(path.join(userDirFor(userId),'model.json'), []); const words=String(note).toLowerCase().split(/[^\p{L}\p{N}]+/u).filter(Boolean); for(const w of words){ let rec=model.find(r=>r.word===w); if(!rec){ rec={ word:w, counts:{} }; model.push(rec); } rec.counts[categoryId]=(rec.counts[categoryId]||0)+1; } writeJson(path.join(userDirFor(userId),'model.json'), model); return true; },
  suggestCategoryFromNote(userId, note){ if(!note) return null; const model=readJson(path.join(userDirFor(userId),'model.json'), []); const words=String(note).toLowerCase().split(/[^\p{L}\p{N}]+/u).filter(Boolean); const scores={}; for(const w of words){ const rec=model.find(r=>r.word===w); if(rec&&rec.counts){ for(const [k,v] of Object.entries(rec.counts)){ scores[k]=(scores[k]||0)+Number(v||0); } } } let best=null,bestScore=0; for(const [k,v] of Object.entries(scores)){ if(v>bestScore){best=v;bestScore=v;} } return best; },
  exportAll(userId){ return { categories:this.getCategories(userId), transactions:this.getTransactions(userId), settings:this.getSettings(userId), model: readJson(path.join(userDirFor(userId),'model.json'), []) }; },
  importAll(userId, data){ if(!data||!Array.isArray(data.categories)||!Array.isArray(data.transactions)) return false; const dir=userDirFor(userId); writeJson(path.join(dir,'categories.json'), data.categories); writeJson(path.join(dir,'transactions.json'), data.transactions); if(data.settings){ writeJson(path.join(dir,'settings.json'), { key:'app', ...data.settings }); } if(Array.isArray(data.model)){ writeJson(path.join(dir,'model.json'), data.model); } return true; }
};

// File-based link store for mapping LINE bot user <-> web login user when DATABASE_URL is not set
const SHARED_DATA_DIR = path.resolve(DATA_BASE, '..'); // e.g. data/
function sharedPath(name){ ensureDir(SHARED_DATA_DIR); return path.join(SHARED_DATA_DIR, name); }
const linkStore = {
  getLinkedWebUser(lineUserId){
    try{
      const map = readJson(sharedPath('links.json'), {});
      return map[lineUserId] || null;
    }catch(_){ return null; }
  },
  upsertLink(lineUserId, webUserId){
    try{
      const map = readJson(sharedPath('links.json'), {});
      map[lineUserId] = webUserId;
      writeJson(sharedPath('links.json'), map);
      return true;
    }catch(_){ return false; }
  },
  createLinkCode(lineUserId, ttlSeconds=300){
    try{
      const codes = readJson(sharedPath('link_codes.json'), {});
      const code = (Math.random().toString(36).slice(2,8)+Math.random().toString(36).slice(2,8)).slice(0,10);
      const exp = Date.now() + ttlSeconds*1000;
      codes[code] = { line_user_id: lineUserId, expires_at: exp };
      writeJson(sharedPath('link_codes.json'), codes);
      return code;
    }catch(_){ return ''; }
  },
  consumeLinkCode(code){
    try{
      const codes = readJson(sharedPath('link_codes.json'), {});
      const rec = codes[code];
      if(!rec){ return null; }
      if(!(Number(rec.expires_at)||0 > Date.now())){ delete codes[code]; writeJson(sharedPath('link_codes.json'), codes); return null; }
      const lineUserId = rec.line_user_id || null;
      delete codes[code];
      writeJson(sharedPath('link_codes.json'), codes);
      return lineUserId;
    }catch(_){ return null; }
  }
};

// Simple in-memory session store (for single-instance). For production multi-instance, use shared store.
const sessions = new Map(); // sessionId -> { user, createdAt }

// Train local model using note -> category (per user)
async function trainModelFor(userIdOrUid, note, categoryId){
  try{
    if(!note || !categoryId) return;
    if(isDbEnabled()){
      await pgdb.updateCategoryModelFromNote(userIdOrUid, note, categoryId);
    }else{
      await fileStore.updateCategoryModelFromNote(userIdOrUid||'anonymous', note, categoryId);
    }
  }catch(_){ /* ignore training errors */ }
}

async function backfillAllUsers(){
  try{
    if(isDbEnabled()){
      const p = await pgdb.getPool();
      // list distinct user ids (including null)
      const r = await p.query("select coalesce(user_id,'anonymous') as uid from transactions group by uid limit 10000");
      const uids = r.rows.map(x=> x.uid);
      for(const uid of uids){
        const rows = await pgdb.getTransactions(uid);
        let aiCount = 0;
        for(const t of rows){
          if(!(t && t.note)) continue;
          // skip if older than recent days (YYYY-MM-DD)
          if(MODEL_TRAIN_RECENT_DAYS>0 && t.date){
            try{
              const d = new Date(t.date);
              if(Number.isFinite(d.getTime())){
                const diff = Date.now() - d.getTime();
                if(diff > MODEL_TRAIN_RECENT_DAYS*86400000) continue;
              }
            }catch(_){ }
          }
          let cat = t.categoryId;
          // If configured, ask ChatGPT for category suggestion to enrich local model
          if(MODEL_TRAIN_USE_AI && aiCount < MODEL_TRAIN_AI_MAX_PER_CYCLE){
            try{
              const ai = await aiStructParse(t.note, {});
              if(ai && ai.categoryName){
                const cats = await pgdb.getCategories();
                const hit = cats.find(c=> String(c.name).toLowerCase()===String(ai.categoryName).toLowerCase());
                if(hit) cat = hit.id;
              }
              aiCount++;
            }catch(_){ /* ignore */ }
          }
          if(cat){ await pgdb.updateCategoryModelFromNote(uid, t.note, cat); }
        }
      }
    }else{
      // scan local data users directory
      const base = DATA_BASE;
      let entries=[];
      try{ entries = fs.readdirSync(base, { withFileTypes:true }); }catch(_){ entries=[]; }
      const dirs = entries.filter(d=> d && d.isDirectory && d.isDirectory()).map(d=> d.name);
      for(const name of dirs){
        const uid = name;
        const rows = fileStore.getTransactions(uid);
        let aiCount = 0;
        for(const t of rows){
          if(!(t && t.note)) continue;
          if(MODEL_TRAIN_RECENT_DAYS>0 && t.date){
            try{
              const d = new Date(t.date);
              if(Number.isFinite(d.getTime())){
                const diff = Date.now() - d.getTime();
                if(diff > MODEL_TRAIN_RECENT_DAYS*86400000) continue;
              }
            }catch(_){ }
          }
          let cat = t.categoryId;
          if(MODEL_TRAIN_USE_AI && aiCount < MODEL_TRAIN_AI_MAX_PER_CYCLE){
            try{
              const ai = await aiStructParse(t.note, {});
              if(ai && ai.categoryName){
                const cats = fileStore.getCategories(uid);
                const hit = cats.find(c=> String(c.name).toLowerCase()===String(ai.categoryName).toLowerCase());
                if(hit) cat = hit.id;
              }
              aiCount++;
            }catch(_){ }
          }
          if(cat){ await fileStore.updateCategoryModelFromNote(uid, t.note, cat); }
        }
      }
    }
    return true;
  }catch(_){ return false; }
}

function startModelTrainer(){
  if(MODEL_TRAIN_ON_START){ try{ backfillAllUsers(); }catch(_){ } }
  if(Number.isFinite(MODEL_TRAIN_INTERVAL_MS) && MODEL_TRAIN_INTERVAL_MS>0){
    setInterval(()=>{ try{ backfillAllUsers(); }catch(_){ } }, MODEL_TRAIN_INTERVAL_MS);
  }
}

function b64url(input){
  return Buffer.from(input).toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
}
function sign(value){
  if(!SESSION_SECRET) return '';
  const mac = crypto.createHmac('sha256', SESSION_SECRET);
  mac.update(value);
  return b64url(mac.digest());
}
function parseCookies(req){
  const h = req.headers['cookie']||'';
  const out = {};
  h.split(';').forEach(p=>{
    const m = p.split('=');
    if(m.length>=2){ out[decodeURIComponent(m[0].trim())] = decodeURIComponent(m.slice(1).join('=').trim()); }
  });
  return out;
}
function setCookie(res, name, val, opts={}){
  const parts = [`${encodeURIComponent(name)}=${encodeURIComponent(val)}`];
  parts.push('Path=/');
  if(opts.httpOnly!==false) parts.push('HttpOnly');
  if(opts.secure!==false) parts.push('Secure');
  parts.push(`SameSite=${opts.sameSite||'Lax'}`);
  if(opts.maxAge) parts.push(`Max-Age=${opts.maxAge}`);
  if(opts.domain) parts.push(`Domain=${opts.domain}`);
  if(opts.path) parts.push(`Path=${opts.path}`);
  const prev = res.getHeader('Set-Cookie');
  res.setHeader('Set-Cookie', [...(Array.isArray(prev)?prev:(prev?[prev]:[])), parts.join('; ')]);
}
function clearCookie(res, name){
  const prev = res.getHeader('Set-Cookie');
  res.setHeader('Set-Cookie', [...(Array.isArray(prev)?prev:(prev?[prev]:[])), `${encodeURIComponent(name)}=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Lax`]);
}
function createSigned(value){
  const sig = sign(value);
  return `${value}.${sig}`;
}
function verifySigned(signed){
  const idx = (signed||'').lastIndexOf('.');
  if(idx<=0) return null;
  const value = signed.slice(0, idx);
  const sig = signed.slice(idx+1);
  const expect = sign(value);
  if(!sig || !expect) return null;
  try{
    if(crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(expect))) return value;
  }catch(_){ }
  return null;
}
function getUserFromRequest(req){
  try{
    const cookies = parseCookies(req);
    const raw = cookies['session']||'';
    const signed = verifySigned(raw);
    if(signed){
      // New stateless cookie: value is 'uid:<userId>'
      if(String(signed).startsWith('uid:')){
        const id = String(signed).slice(4);
        if(id) return { id };
      }
      // Legacy: treat as sid and read from in-memory map
      const s = sessions.get(signed);
      if(s && s.user) return s.user;
    }
    // 匿名模式：若允許未登入，改用簽名的 uid cookie 作為使用者 ID
    if(!REQUIRE_AUTH){
      const anon = verifySigned(cookies['uid']||'');
      if(anon){ return { id: anon }; }
    }
    return null;
  }catch(_){ return null; }
}

function reqUserId(req){
  const u = getUserFromRequest(req);
  return (u && u.id) ? u.id : 'anonymous';
}

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Line-Signature, X-Admin-Key');
}

function getBaseUrl(req){
  try{
    const protoHeader = (req.headers['x-forwarded-proto']||'').toString();
    const proto = protoHeader || (req.socket && req.socket.encrypted ? 'https' : 'http');
    const host = (req.headers['x-forwarded-host']||req.headers.host||'').toString();
    if(host){ return `${proto}://${host}`; }
  }catch(_){ }
  return '';
}

// 在允許匿名的情況下，確保每位未登入使用者有固定的簽名 uid cookie（持久化到瀏覽器），避免資料混用
function ensureAnonCookie(req, res){
  if(REQUIRE_AUTH) return;
  try{
    const cookies = parseCookies(req);
    const existing = verifySigned(cookies['uid']||'');
    if(existing) return; // 已存在有效 uid
    const anonId = 'anon:' + crypto.randomBytes(12).toString('hex');
    const isHttps = /^https:\/\//.test(getBaseUrl(req)||'');
    setCookie(res, 'uid', createSigned(anonId), { maxAge: 60*60*24*365, secure: isHttps });
  }catch(_){ /* ignore */ }
}

function parseBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > 1_000_000) {
        reject(new Error('Payload too large'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      const raw = Buffer.concat(chunks);
      resolve(raw);
    });
    req.on('error', reject);
  });
}

function verifyLineSignature(rawBody, signature) {
  if (!LINE_CHANNEL_SECRET) return true; // skip if not configured
  if (!signature) return false;
  try{
    const hmac = crypto.createHmac('sha256', LINE_CHANNEL_SECRET);
    hmac.update(rawBody);
    const digest = hmac.digest('base64');
    const a = Buffer.from(signature, 'base64');
    const b = Buffer.from(digest, 'base64');
    if(a.length !== b.length) return false;
    return crypto.timingSafeEqual(a, b);
  }catch(_){
    return false;
  }
}

async function lineReply(replyToken, messages){
  if(!LINE_CHANNEL_ACCESS_TOKEN){
    try{ console.warn && console.warn('[line] reply skipped: missing LINE_CHANNEL_ACCESS_TOKEN'); }catch(_){ }
    return false;
  }
  const payload = { replyToken, messages };
  try{
    await fetchJson('https://api.line.me/v2/bot/message/reply', {
      method:'POST',
      headers:{ 'Content-Type':'application/json', 'Authorization': `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}` },
      body: JSON.stringify(payload)
    }, 10000);
    return true;
  }catch(_){ return false; }
}

async function linePush(to, messages){
  if(!LINE_CHANNEL_ACCESS_TOKEN){
    try{ console.warn && console.warn('[line] push skipped: missing LINE_CHANNEL_ACCESS_TOKEN'); }catch(_){ }
    return false;
  }
  const payload = { to, messages };
  try{
    await fetchJson('https://api.line.me/v2/bot/message/push', {
      method:'POST', headers:{ 'Content-Type':'application/json', 'Authorization': `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}` }, body: JSON.stringify(payload)
    }, 10000);
    return true;
  }catch(_){ return false; }
}

// Build a glass-style Flex bubble approximating iOS frosted glass aesthetics
function glassFlexBubble({ baseUrl='', title='', subtitle='', lines=[], buttons=[], showHero=false, compact=true }){
  const isHttps = /^https:\/\//.test(baseUrl||'');
  const heroUrl = (showHero && isHttps) ? `${baseUrl}/flex-glass.svg` : '';
  const bodyContents = [];
  if(title){ bodyContents.push({ type:'text', text:String(title), weight:'bold', size: compact?'lg':'xl', color:'#0f172a' }); }
  if(subtitle){ bodyContents.push({ type:'text', text:String(subtitle), size: compact?'xs':'sm', color:'#475569', wrap:true, margin: compact?'xs':'sm' }); }
  if(Array.isArray(lines) && lines.length>0){
    bodyContents.push({
      type:'box', layout:'vertical', spacing: compact?'xs':'sm', paddingAll: compact?'8px':'12px', backgroundColor:'#ffffffcc', cornerRadius:'12px', contents:
        lines.map(t=>({ type:'text', text:String(t), size: compact?'xs':'sm', color:'#0f172a', wrap:true }))
    });
  }
  const footerContents = (buttons||[]).map(btn=>({
    type:'button',
    style: btn.style||'primary',
    color: btn.color||'#0ea5e9',
    action: btn.action
  }));
  return {
    type:'bubble',
    hero: heroUrl ? { type:'image', url: heroUrl, size:'full', aspectRatio:'20:10', aspectMode:'cover' } : undefined,
    body:{ type:'box', layout:'vertical', spacing: compact?'xs':'sm', contents: bodyContents, paddingAll: compact?'12px':'16px' },
    footer: footerContents.length>0 ? { type:'box', layout:'vertical', spacing: compact?'sm':'md', contents: footerContents, flex:0, paddingAll: compact?'12px':'16px' } : undefined
  };
}

// Build main menu Flex bubble for LINE bot
function menuFlexBubble({ baseUrl='' }){
  const rows = [
    [
      { label:'記一筆', data:'flow=add&step=start', style:'primary' },
      { label:'最近交易', text:'最近交易', style:'secondary' }
    ],
    [
      { label:'未請款', text:'未請款', style:'secondary' },
      { label:'分類支出', text:'分類支出', style:'secondary' }
    ],
    [
      { label:'統計摘要', text:'查帳', style:'secondary' },
      { label:'開啟網頁版', uri: baseUrl||'https://example.com', style:'link' }
    ]
  ];
  const contents = [];
  contents.push({ type:'text', text:'功能選單', weight:'bold', size:'lg', color:'#0f172a' });
  contents.push({ type:'text', text:'在這裡可以快速記帳或查看資訊', size:'xs', color:'#475569', wrap:true, margin:'xs' });
  for(const row of rows){
    contents.push({
      type:'box', layout:'horizontal', spacing:'md', contents: row.map(btn=>{
        if(btn.data){
          return { type:'button', style: btn.style==='link'?'link':'primary', color: btn.style==='secondary'?'#64748b':undefined, action:{ type:'postback', label:btn.label, data:btn.data } };
        }
        if(btn.text){
          return { type:'button', style: btn.style==='link'?'link':'secondary', color: btn.style==='secondary'?'#64748b':undefined, action:{ type:'message', label:btn.label, text:btn.text } };
        }
        if(btn.uri){
          return { type:'button', style:'link', action:{ type:'uri', label:btn.label, uri: btn.uri } };
        }
        return { type:'spacer' };
      })
    });
  }
  return {
    type:'bubble',
    body:{ type:'box', layout:'vertical', spacing:'xs', contents, paddingAll:'12px' }
  };
}

// Simple in-memory guided add flow state
const guidedFlow = new Map(); // userId -> { step, payload }
const aiFeatureFlow = new Map(); // userId -> { kind, step }

// AI contextual intent support (in-memory, single instance)
const aiPending = new Map(); // actionId -> { userId, kind: 'add_tx'|'delete_tx', payload?, txId? }
const aiLastTxId = new Map(); // userId -> last transaction id added via AI
const adminState = new Map(); // userId -> { mode: 'broadcast_wait' }

function newActionId(){
  try{ return crypto.randomBytes(10).toString('hex'); }catch(_){ return String(Date.now())+Math.random().toString(16).slice(2,10); }
}

function summarizeTxLines(tx, cats){
  const name = (cats && Array.isArray(cats)) ? (cats.find(c=>c.id===tx.categoryId)?.name || tx.categoryId) : (tx.categoryId||'');
  const lines = [
    `日期：${tx.date||todayYmd()}`,
    `金額：${tx.currency||'TWD'} ${Number(tx.amount||0).toFixed(2)}`,
    name ? `分類：${name}` : undefined,
    tx.note ? `備註：${String(tx.note).slice(0,60)}` : undefined
  ].filter(Boolean);
  return lines;
}

function buildAiConfirmAddBubble(base, payload, actionId){
  return glassFlexBubble({
    baseUrl: base,
    title: '偵測到支出，是否新增？',
    subtitle: 'AI 已解析，請確認或調整',
    lines: summarizeTxLines({ ...payload }, null),
    buttons: [
      { style:'primary', action:{ type:'postback', label:'確認新增', data:`flow=ai&step=add&action=${encodeURIComponent(actionId)}&do=1` } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'選擇分類', data:`flow=ai&step=choose_cat&action=${encodeURIComponent(actionId)}` } },
      { style:'link', action:{ type:'postback', label:'取消', data:`flow=ai&step=cancel&action=${encodeURIComponent(actionId)}` } }
    ],
    showHero:false,
    compact:true
  });
}

function buildAiConfirmDeleteBubble(base, tx, actionId){
  return glassFlexBubble({
    baseUrl: base,
    title: '是否刪除上一筆？',
    subtitle: '偵測到你想撤回/更正',
    lines: summarizeTxLines(tx, null),
    buttons: [
      { style:'primary', color:'#dc2626', action:{ type:'postback', label:'確認刪除', data:`flow=ai&step=delete&action=${encodeURIComponent(actionId)}&do=1` } },
      { style:'link', action:{ type:'postback', label:'取消', data:`flow=ai&step=cancel&action=${encodeURIComponent(actionId)}` } }
    ],
    showHero:false,
    compact:true
  });
}

function isLineAdmin(lineUidRaw){
  try{ return !!lineUidRaw && (String(lineUidRaw)===String(ADMIN_LINE_USER_ID)); }catch(_){ return false; }
}

function adminMenuBubble({ baseUrl='' }){
  return glassFlexBubble({
    baseUrl,
    title:'管理選單',
    subtitle:'僅管理者可用',
    lines:['可執行系統廣播與模型管理'],
    buttons:[
      { style:'primary', action:{ type:'postback', label:'發送廣播', data:'flow=admin&step=broadcast' } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'回填訓練', data:'flow=admin&step=backfill' } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'查看模型', data:'flow=admin&step=inspect' } },
      { style:'link', action:{ type:'postback', label:'關閉', data:'flow=admin&step=cancel' } }
    ],
    showHero:false,
    compact:true
  });
}

// Edit (multi-turn) state for LINE
const editState = new Map(); // userId -> { txId, step: 'edit_amount'|'edit_date'|'edit_note' }

async function getLastTransactionRecord(userIdOrUid){
  try{
    if(isDbEnabled()){
      const rows = await pgdb.getTransactions(userIdOrUid);
      return rows && rows[0] ? rows[0] : null;
    }else{
      const rows = fileStore.getTransactions(userIdOrUid||'anonymous');
      return rows && rows[0] ? rows[0] : null;
    }
  }catch(_){ return null; }
}

function buildEditMenuBubble(base, tx, txId){
  return glassFlexBubble({
    baseUrl: base,
    title: '編輯這筆交易',
    subtitle: tx?.date || todayYmd(),
    lines: summarizeTxLines(tx||{}, null),
    buttons: [
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'改分類', data:`flow=edit&step=choose_cat&tx=${encodeURIComponent(txId)}` } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'改金額', data:`flow=edit&step=amount&tx=${encodeURIComponent(txId)}` } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'改日期', data:`flow=edit&step=date&tx=${encodeURIComponent(txId)}` } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'改備註', data:`flow=edit&step=note&tx=${encodeURIComponent(txId)}` } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'標記已請款', data:`flow=edit&step=claimed&tx=${encodeURIComponent(txId)}&val=1` } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'標記未請款', data:`flow=edit&step=claimed&tx=${encodeURIComponent(txId)}&val=0` } },
      { style:'link', action:{ type:'postback', label:'完成', data:`flow=edit&step=cancel&tx=${encodeURIComponent(txId)}` } }
    ],
    showHero:false,
    compact:true
  });
}

function buildEditPromptBubble(base, kind, txId){
  const title = kind==='amount' ? '輸入新金額'
    : kind==='date' ? '輸入新日期'
    : '輸入新備註';
  const subtitle = kind==='amount' ? '範例：120 或 85.5'
    : kind==='date' ? '格式：YYYY-MM-DD，或「今天/昨天/前天/明天」'
    : '請直接輸入文字（最多 200 字）';
  return glassFlexBubble({
    baseUrl: base,
    title, subtitle,
    lines: [],
    buttons: [ { style:'link', action:{ type:'postback', label:'取消', data:`flow=edit&step=cancel&tx=${encodeURIComponent(txId)}` } } ],
    showHero:false,
    compact:true
  });
}

async function handleContextualAI(req, replyToken, userId, lineUidRaw, text){
  try{
    const t = String(text||'');
    const base = getBaseUrl(req)||'';
    const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
    const lower = t.toLowerCase();

    // Case A: possible fraud/large unexpected loss
    const isFraud = /(詐騙|被騙|盜刷|詐欺|騙走)/.test(t);
    if(isFraud){
      // Prefer AI struct parse to extract amount/date/note
      let catCtx = {};
      try{
        const cats = await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(userId||'anonymous'));
        catCtx = { categories: cats };
      }catch(_){ catCtx = {}; }
      let parsed = await aiStructParse(t, catCtx);
      parsed = mergeParsedAmountFromText(t, parsed||{});
      if(!parsed){ parsed = parseNlpQuick(t); }
      const amt = Number(parsed?.amount||0);
      if(Number.isFinite(amt) && amt>0){
        const payload = {
          date: parsed.date || todayYmd(),
          type: 'expense',
          categoryId: '', // 由使用者後續選擇或使用預設
          currency: parsed.currency || 'TWD',
          rate: Number(parsed.rate)||1,
          amount: amt,
          claimAmount: Number(parsed.claimAmount)||0,
          claimed: parsed.claimed===true,
          note: String(parsed.note||t||'')
        };
        const actionId = newActionId();
        aiPending.set(actionId, { userId: uid, kind:'add_tx', payload });
        const bubble = buildAiConfirmAddBubble(base, payload, actionId);
        await lineReply(replyToken, [{ type:'flex', altText:'確認新增', contents:bubble }]);
        return true;
      }
    }

    // Case B: user says it was a misunderstanding/cancel → offer to delete last AI-added tx
    const isRevert = /(誤會|不用了|取消|撤回|搞錯|不是)/.test(t);
    if(isRevert){
      const lastId = aiLastTxId.get(uid);
      if(lastId){
        let tx = null;
        try{
          if(isDbEnabled()) tx = await pgdb.getTransactionById(uid, lastId);
          else tx = fileStore.getTransactionById(uid, lastId);
        }catch(_){ tx=null; }
        if(tx){
          const actionId = newActionId();
          aiPending.set(actionId, { userId: uid, kind:'delete_tx', txId: lastId });
          const bubble = buildAiConfirmDeleteBubble(base, tx, actionId);
          await lineReply(replyToken, [{ type:'flex', altText:'確認刪除', contents:bubble }]);
          return true;
        }
      }
    }
  }catch(_){ }
  return false;
}

function parsePostbackData(s){
  const params = new URLSearchParams(String(s||''));
  const out = {};
  for(const [k,v] of params.entries()){ out[k]=v; }
  return out;
}

function buildAmountPrompt(base){
  const amounts = [50,100,150,200,300,500];
  return glassFlexBubble({
    baseUrl: base,
    title:'輸入金額',
    subtitle:'請輸入金額，或點選快速金額',
    lines:[ '範例：120、85.5' ],
    buttons: amounts.map(v=>({ style:'secondary', color:'#64748b', action:{ type:'postback', label:`$${v}`, data:`flow=add&step=amount&value=${v}` } })).concat([
      { style:'link', action:{ type:'postback', label:'取消', data:'flow=add&step=cancel' } }
    ]),
    showHero:false,
    compact:true
  });
}

async function buildCategoryPrompt(base, isDb, userIdOrUid){
  let cats=[];
  try{ cats = isDb ? (await pgdb.getCategories()) : fileStore.getCategories(userIdOrUid||'anonymous'); }catch(_){ cats=[]; }
  const top = cats.slice(0,10);
  const buttons = top.map(c=> ({ style:'secondary', color:'#64748b', action:{ type:'postback', label:c.name, data:`flow=add&step=cat&id=${encodeURIComponent(c.id)}` } }));
  return glassFlexBubble({
    baseUrl: base,
    title:'選擇分類',
    subtitle:'請選擇分類',
    lines:[],
    buttons: buttons.concat([{ style:'link', action:{ type:'postback', label:'取消', data:'flow=add&step=cancel' } }]),
    showHero:false,
    compact:true
  });
}

async function buildAiChooseCategoryPrompt(base, isDb, userIdOrUid, actionId){
  let cats=[];
  try{ cats = isDb ? (await pgdb.getCategories()) : fileStore.getCategories(userIdOrUid||'anonymous'); }catch(_){ cats=[]; }
  const top = cats.slice(0,10);
  const buttons = top.map(c=> ({ style:'secondary', color:'#64748b', action:{ type:'postback', label:c.name, data:`flow=ai&step=choose_cat_do&action=${encodeURIComponent(actionId)}&id=${encodeURIComponent(c.id)}` } }));
  return glassFlexBubble({
    baseUrl: base,
    title:'選擇分類',
    subtitle:'請選擇分類以完成新增',
    lines:[],
    buttons: buttons.concat([{ style:'link', action:{ type:'postback', label:'取消', data:`flow=ai&step=cancel&action=${encodeURIComponent(actionId)}` } }]),
    showHero:false,
    compact:true
  });
}

async function buildEditChooseCategoryPrompt(base, isDb, userIdOrUid, txId){
  let cats=[];
  try{ cats = isDb ? (await pgdb.getCategories()) : fileStore.getCategories(userIdOrUid||'anonymous'); }catch(_){ cats=[]; }
  const top = cats.slice(0,10);
  const buttons = top.map(c=> ({ style:'secondary', color:'#64748b', action:{ type:'postback', label:c.name, data:`flow=edit&step=choose_cat_do&tx=${encodeURIComponent(txId)}&id=${encodeURIComponent(c.id)}` } }));
  return glassFlexBubble({
    baseUrl: base,
    title:'選擇分類',
    subtitle:'請選擇新的分類',
    lines:[],
    buttons: buttons.concat([{ style:'link', action:{ type:'postback', label:'取消', data:`flow=edit&step=cancel&tx=${encodeURIComponent(txId)}` } }]),
    showHero:false,
    compact:true
  });
}

function buildNotePrompt(base){
  return glassFlexBubble({
    baseUrl: base,
    title:'備註（可略過）',
    subtitle:'請直接輸入備註文字，或點選略過',
    lines:[ '若不需要可點「略過」' ],
    buttons:[ { style:'secondary', color:'#64748b', action:{ type:'postback', label:'略過', data:'flow=add&step=note&skip=1' } }, { style:'link', action:{ type:'postback', label:'取消', data:'flow=add&step=cancel' } } ],
    showHero:false,
    compact:true
  });
}

function buildClaimAskPrompt(base){
  return glassFlexBubble({
    baseUrl: base,
    title: '是否新增請款金額？',
    subtitle: '若需要報帳，請選「是」並輸入請款金額',
    lines: [],
    buttons: [
      { style:'primary', action:{ type:'postback', label:'是，新增請款金額', data:'flow=add&step=claim_ask&ans=yes' } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'否，略過', data:'flow=add&step=claim_ask&ans=no' } },
      { style:'link', action:{ type:'postback', label:'取消', data:'flow=add&step=cancel' } }
    ],
    showHero:false,
    compact:true
  });
}

function buildClaimAmountPrompt(base, total){
  const amt = Number(total)||0;
  const candidatesRaw = Array.from(new Set([
    Math.max(1, Math.round(amt)),
    Math.max(1, Math.round(amt*0.8)),
    Math.max(1, Math.round(amt*0.5))
  ])).slice(0,3);
  const buttons = candidatesRaw.map(v=>({ style:'secondary', color:'#64748b', action:{ type:'postback', label:`$${v}`, data:`flow=add&step=claim_amount&value=${v}` } }));
  buttons.push({ style:'secondary', color:'#64748b', action:{ type:'postback', label:'略過', data:'flow=add&step=claim_ask&ans=no' } });
  buttons.push({ style:'link', action:{ type:'postback', label:'取消', data:'flow=add&step=cancel' } });
  return glassFlexBubble({
    baseUrl: base,
    title: '輸入請款金額',
    subtitle: '請輸入數字，或點選快速金額',
    lines: [ amt>0 ? `本次金額：$${amt.toFixed(2)}` : '' ].filter(Boolean),
    buttons,
    showHero:false,
    compact:true
  });
}

function parseNlpQuick(text){
  const t = String(text||'').trim();
  const result = { };
  const normalized = t.replace(/，/g, ',');
  // type
  if(/(^|\s)(支出|花費|付|花|扣)($|\s)/.test(t)) result.type = 'expense';
  if(/(^|\s)(收入|入帳|收)($|\s)/.test(t)) result.type = 'income';
  // currency synonyms
  const currencySynonyms = [
    ['TWD', /(TWD|NTD|NT\$|NT|台幣|新台幣|元|塊)/i],
    ['USD', /(USD|美金|美元)/i],
    ['JPY', /(JPY|日幣|日元)/i],
    ['EUR', /(EUR|歐元)/i],
    ['CNY', /(CNY|人民幣|RMB)/i],
    ['HKD', /(HKD|港幣)/i]
  ];
  for(const [code, rx] of currencySynonyms){ if(rx.test(t)){ result.currency = code; break; } }
  // chinese numerals → number
  function chineseToNumber(input){
    if(!input) return NaN;
    const digit = { '零':0,'〇':0,'一':1,'二':2,'兩':2,'三':3,'四':4,'五':5,'六':6,'七':7,'八':8,'九':9 };
    const unit = { '十':10,'百':100,'千':1000,'萬':10000 };
    let total = 0, section = 0, number = 0;
    for(const ch of input){
      if(digit.hasOwnProperty(ch)){
        number = digit[ch];
      }else if(unit.hasOwnProperty(ch)){
        const u = unit[ch];
        if(u === 10000){ section += (number||0); total += section * 10000; section = 0; number = 0; }
        else { section += (number||1) * u; number = 0; }
      }
    }
    return total + section + (number||0);
  }
  function parseAmount(str){
    const raw = String(str||'');
    // Remove obvious date/time/ordinal contexts to avoid false positives like "十月" -> 10
    let s = raw
      // Full/partial dates
      .replace(/\b20\d{2}[-\/\.年]\d{1,2}[-\/\.月]\d{1,2}(?:日)?\b/g, ' ')
      .replace(/\b\d{1,2}\/\d{1,2}\b/g, ' ')
      // Times like 10:30 or 10：30
      .replace(/\b\d{1,2}[:：]\d{2}(?:[:：]\d{2})?\b/g, ' ')
      // Chinese numerals followed by time/date/counter units
      .replace(/([零〇一二兩三四五六七八九十百千萬]+)\s*(月|日|號|号|點|点|時|小時|分鐘|分|秒|年|週|周|星期|禮拜|樓|層|次|件|篇|張|號|号)/g, ' ')
      // Arabic numerals followed by the same units
      .replace(/([0-9]+)\s*(月|日|號|号|點|点|時|小時|分鐘|分|秒|年|週|周|星期|禮拜|樓|層|次|件|篇|張|號|号)/g, ' ')
      // Ordinals like 第十次、第10次
      .replace(/第\s*([零〇一二兩三四五六七八九十百千萬]+|[0-9]+)\s*(次|筆|條|篇|項|名|個|位)/g, ' ');

    // Pass 1: amount with explicit currency unit (e.g., 120元 / 十元)
    const unitNum = s.match(/([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*(元|塊|圓|块|塊錢)/);
    if(unitNum){ return Number(unitNum[1].replace(/,/g,'')); }
    const unitCjk = s.match(/([零〇一二兩三四五六七八九十百千萬]+)\s*(元|塊|圓|块|塊錢)/);
    if(unitCjk){ const v = chineseToNumber(unitCjk[1]); if(Number.isFinite(v)) return v; }

    // Pass 2: currency code/synonym followed by number (e.g., NT$ 120, 台幣120)
    const curLeading = s.match(/\b(TWD|NTD|NT\$|NT|台幣|新台幣)\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\b/i);
    if(curLeading){ return Number(curLeading[2].replace(/,/g,'')); }
    const curTrailing = s.match(/\b([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*(TWD|NTD|NT\$|NT|台幣|新台幣)\b/i);
    if(curTrailing){ return Number(curTrailing[1].replace(/,/g,'')); }

    // Pass 3: plain number (prefer long digits, then thousands with comma)
    const plain = s.match(/([0-9]+(?:\.[0-9]+)?|[0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]+)?)/);
    if(plain){ return Number(plain[1].replace(/,/g,'')); }
    return undefined;
  }
  // amount
  const amount = parseAmount(normalized);
  if(Number.isFinite(amount)) result.amount = amount;
  // rate
  const rate = normalized.match(/匯率\s*([0-9]+(?:\.[0-9]+)?)/);
  if(rate) result.rate = Number(rate[1]);
  // date: ISO or relative
  const iso = normalized.match(/(20\d{2})[-\/]?(\d{1,2})[-\/]?(\d{1,2})/);
  if(iso){ const y=Number(iso[1]); const m=String(Number(iso[2])).padStart(2,'0'); const d=String(Number(iso[3])).padStart(2,'0'); result.date=`${y}-${m}-${d}`; }
  if(!result.date){
    // 支援 MM/DD（無年份）→ 以今年為準
    const md = normalized.match(/\b(\d{1,2})\/(\d{1,2})\b/);
    if(md){
      const now = new Date();
      const y = now.getFullYear();
      const m = String(Number(md[1])).padStart(2,'0');
      const d = String(Number(md[2])).padStart(2,'0');
      result.date = `${y}-${m}-${d}`;
    }
  }
  if(!result.date){
    const now = new Date();
    const fmt = (d)=> `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
    if(/今天/.test(t)) result.date = fmt(new Date());
    else if(/昨天|昨日/.test(t)){ const d=new Date(now); d.setDate(d.getDate()-1); result.date = fmt(d); }
    else if(/前天/.test(t)){ const d=new Date(now); d.setDate(d.getDate()-2); result.date = fmt(d); }
    else if(/明天/.test(t)){ const d=new Date(now); d.setDate(d.getDate()+1); result.date = fmt(d); }
  }
  // claim amount
  const claim = normalized.match(/請款\s*([0-9,\.零〇一二兩三四五六七八九十百千萬]+)/);
  if(claim){ const v = parseAmount(claim[1]); if(Number.isFinite(v)) result.claimAmount = v; }
  // synonyms: 不用請款 / 無需請款 / 不報帳 / 不需報帳 / 免請款
  if(/已請款|完成請款|報帳完成/.test(t)) result.claimed = true;
  if(/未請款|還沒請款|不用請款|無需請款|不報帳|不需報帳|免請款/.test(t)) result.claimed = false;
  // note
  result.note = t;
  return result;
}

function mergeParsedAmountFromText(text, aiParsed){
  try{
    const local = parseNlpQuick(text);
    const merged = Object.assign({}, aiParsed||{});
    const localAmt = Number(local?.amount);
    const aiAmt = Number(aiParsed?.amount);
    const hasLocal = Number.isFinite(localAmt) && localAmt>0;
    if(hasLocal){
      merged.amount = localAmt;
    }else{
      const raw = String(text||'');
      // sanitize text by removing obvious date/time/ordinal contexts
      const t = raw
        .replace(/\b20\d{2}[-\/\.年]\d{1,2}[-\/\.月]\d{1,2}(?:日)?\b/g, ' ')
        .replace(/\b\d{1,2}\/\d{1,2}\b/g, ' ')
        .replace(/\b\d{1,2}[:：]\d{2}(?:[:：]\d{2})?\b/g, ' ')
        .replace(/第\s*([零〇一二兩三四五六七八九十百千萬]+|[0-9]+)\s*(次|筆|條|篇|項|名|個|位)/g, ' ')
        .replace(/([零〇一二兩三四五六七八九十百千萬]+)\s*(月|日|號|号|點|点|時|小時|分鐘|分|秒|年|週|周|星期|禮拜|樓|層|次|件|篇|張|號|号)/g, ' ')
        .replace(/([0-9]+)\s*(月|日|號|号|點|点|時|小時|分鐘|分|秒|年|週|周|星期|禮拜|樓|層|次|件|篇|張|號|号)/g, ' ');
      const aiNum = Number.isFinite(aiAmt) && aiAmt>0 ? String(aiAmt) : '';
      let ok = false;
      if(aiNum){
        const unitAfter = new RegExp(`(?:^|[^0-9])${aiNum}\\s*(元|塊|圓|块|塊錢)(?:[^0-9]|$)`);
        const unitBefore = new RegExp(`(TWD|NTD|NT\\$|NT|台幣|新台幣)\\s*${aiNum}(?:[^0-9]|$)`, 'i');
        // 僅在有明確金額單位或幣別時才接受 AI 金額
        ok = unitAfter.test(t) || unitBefore.test(t);
      }
      if(ok){ merged.amount = aiAmt; } else { delete merged.amount; }
    }
    if(local?.currency && !merged.currency){ merged.currency = local.currency; }
    if(local?.date && !merged.date){ merged.date = local.date; }
    return merged;
  }catch{ return aiParsed||null; }
}

function validateAndNormalizeStruct(input){
  try{
    const o = input && typeof input==='object' ? input : {};
    const out = {};
    // type
    const t = String(o.type||'').toLowerCase();
    out.type = (t==='income' || t==='expense') ? t : 'expense';
    // amount (must be finite and >0)
    const amt = Number(o.amount);
    if(Number.isFinite(amt) && amt>0){ out.amount = amt; }
    // currency (normalize casing)
    if(o.currency){ out.currency = String(o.currency).toUpperCase(); }
    // date (YYYY-MM-DD)
    if(o.date && /^(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/.test(String(o.date))){ out.date = String(o.date); }
    // rate
    const rate = Number(o.rate);
    if(Number.isFinite(rate) && rate>0){ out.rate = rate; }
    // claimAmount
    const camt = Number(o.claimAmount);
    if(Number.isFinite(camt) && camt>=0){ out.claimAmount = camt; }
    // claimed
    if(typeof o.claimed==='boolean'){ out.claimed = o.claimed; }
    // categoryName
    if(o.categoryName){ out.categoryName = String(o.categoryName).trim().slice(0,80); }
    // note
    if(o.note){ out.note = String(o.note).trim().slice(0,500); }
    return out;
  }catch{ return {}; }
}

async function aiStructParse(text, context){
  const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';
  const OPENAI_BASE_URL = process.env.OPENAI_BASE_URL || 'https://api.openai.com/v1';
  const OPENAI_MODEL = process.env.OPENAI_MODEL || 'gpt-4o-mini';
  if(!OPENAI_API_KEY){ return null; }
  const catNames = Array.isArray(context?.categories) ? context.categories.map(c=>String(c.name)).slice(0,100) : [];
  const categoriesHint = catNames.length>0 ? `有效分類（優先從此清單選擇 categoryName；對不到可輸出新的文字並由前端建立）：${catNames.join(', ')}` : '';
  const payload = {
    model: OPENAI_MODEL,
    messages: [
      { role:'system', content: `你是專業的記帳解析器，輸出嚴格 JSON（無多餘文字）。
欄位: type(income|expense; 預設 expense), amount(number), currency(string; 例如 TWD USD), date(YYYY-MM-DD; 支援 今天/昨天/前天/明天，相對日期；若輸入為 MM/DD 則年份取今年), categoryName(string), rate(number 可省略), claimAmount(number 可省略), claimed(boolean 可省略), note(string)。
金額可含逗號或中文數字（如 一百二十/兩百），幣別同義字（台幣/新台幣/NT/NTD/NT$ 視為 TWD）。
請根據提供的分類清單選擇 categoryName；對不到可輸出新的 categoryName（純文字），前端會自動建立分類。
「不用請款/無需請款/不報帳/不需報帳/免請款」→ claimed=false 並 claimAmount=0；「已請款/完成請款/報帳完成」→ claimed=true。
${categoriesHint}
嚴禁將日期/時間/序數視為金額（例如：十月、十點、10:30、10/31、第三次）。若無法確定金額，省略 amount 欄位，切勿臆測。
只輸出 JSON，不要其他文字。` },
      { role:'user', content: text }
    ],
    temperature: 0
  };
  const base = (OPENAI_BASE_URL||'').replace(/\/+$/,'');
  const apiBase = /\/v\d+(?:$|\/)/.test(base) ? base : `${base}/v1`;
  const endpoint = `${apiBase}/chat/completions`;
  try{
    const data = await fetchJson(endpoint, {
      method:'POST',
      headers:{ 'Authorization':`Bearer ${OPENAI_API_KEY}`, 'Content-Type':'application/json' },
      body: JSON.stringify(payload)
    }, 20000);
    const reply = data?.choices?.[0]?.message?.content || '';
    try{ return JSON.parse(reply); }catch(_){ return null; }
  }catch(_){ return null; }
}

async function aiChatText(userText, context){
  try{
    const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';
    const OPENAI_BASE_URL = process.env.OPENAI_BASE_URL || 'https://api.openai.com/v1';
    const OPENAI_MODEL = process.env.OPENAI_MODEL || 'gpt-4o-mini';
    if(!OPENAI_API_KEY){
      return heuristicReply([{ role:'user', content:String(userText||'') }], context||{});
    }
    const payload = {
      model: OPENAI_MODEL,
      messages: [
        { role: 'system', content: 'You are a helpful finance and budgeting assistant for a personal ledger web app. Answer in Traditional Chinese.' },
        { role: 'system', content: `Context JSON (may be partial): ${JSON.stringify(context||{}).slice(0, 4000)}` },
        { role: 'user', content: String(userText||'') }
      ],
      temperature: 0.4
    };
    const base = (OPENAI_BASE_URL||'').replace(/\/+$/,'');
    const apiBase = /\/v\d+(?:$|\/)/.test(base) ? base : `${base}/v1`;
    const endpoint = `${apiBase}/chat/completions`;
    const data = await fetchJson(endpoint, {
      method:'POST', headers:{ 'Authorization':`Bearer ${OPENAI_API_KEY}`, 'Content-Type':'application/json' }, body: JSON.stringify(payload)
    }, 20000);
    if(!data || data.error){
      return heuristicReply([{ role:'user', content:String(userText||'') }], context||{});
    }
    const reply = data?.choices?.[0]?.message?.content || '';
    return reply || heuristicReply([{ role:'user', content:String(userText||'') }], context||{});
  }catch(_){
    return heuristicReply([{ role:'user', content:String(userText||'') }], context||{});
  }
}

async function aiOpsParse(text, context){
  const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';
  const OPENAI_BASE_URL = process.env.OPENAI_BASE_URL || 'https://api.openai.com/v1';
  const OPENAI_MODEL = process.env.OPENAI_MODEL || 'gpt-4o-mini';
  if(!OPENAI_API_KEY){ return null; }
  const catNames = Array.isArray(context?.categories) ? context.categories.map(c=>String(c.name)).slice(0,100) : [];
  const categoriesHint = catNames.length>0 ? `已知分類：${catNames.join(', ')}` : '';
  const payload = {
    model: OPENAI_MODEL,
    messages: [
      { role:'system', content: `你是記帳系統的操作代理，務必輸出嚴格 JSON（無多餘文字）。
結構：{ op: 'add'|'update'|'delete'|'stats', criteria?: { date?: 'YYYY-MM-DD' 或 'MM/DD' 或 '今天/昨天/前天', amount?: number, noteContains?: string, categoryName?: string }, patch?: { amount?: number, date?: 'YYYY-MM-DD', note?: string, categoryName?: string, type?: 'income'|'expense', currency?: string, claimAmount?: number, claimed?: boolean } }。
規則：
- 若使用者表示要「更正/改/修正」，輸出 op='update'，將欲更改的新值放在 patch，將原本用來辨識哪一筆的條件放到 criteria。
- 若表示要刪除某筆，輸出 op='delete' 並附上 criteria。
- 若是單純新增紀錄，輸出 op='add'，其餘欄位略過（新增另有結構化流程）。
- ${categoriesHint}
只輸出 JSON。` },
      { role:'user', content: text }
    ],
    temperature: 0
  };
  const base = (OPENAI_BASE_URL||'').replace(/\/+$/,'');
  const apiBase = /\/v\d+(?:$|\/)/.test(base) ? base : `${base}/v1`;
  const endpoint = `${apiBase}/chat/completions`;
  try{
    const data = await fetchJson(endpoint, {
      method:'POST', headers:{ 'Authorization':`Bearer ${OPENAI_API_KEY}`, 'Content-Type':'application/json' }, body: JSON.stringify(payload)
    }, 20000);
    const reply = data?.choices?.[0]?.message?.content || '';
    try{ return JSON.parse(reply); }catch(_){ return null; }
  }catch(_){ return null; }
}

function validateAndNormalizeOps(input){
  try{
    const o = input && typeof input==='object' ? input : {};
    const out = {};
    const op = String(o.op||'').toLowerCase();
    if(['add','update','delete','stats'].includes(op)) out.op = op; else return null;
    const critIn = o.criteria||{}; const patchIn = o.patch||{};
    const criteria = {};
    if(critIn.date){ criteria.date = String(critIn.date).trim().slice(0,20); }
    const amt = Number(critIn.amount); if(Number.isFinite(amt) && amt>0) criteria.amount = amt;
    if(critIn.noteContains){ criteria.noteContains = String(critIn.noteContains).trim().slice(0,80); }
    if(critIn.categoryName){ criteria.categoryName = String(critIn.categoryName).trim().slice(0,80); }
    const patch = {};
    const pAmt = Number(patchIn.amount); if(Number.isFinite(pAmt) && pAmt>0) patch.amount = pAmt;
    if(patchIn.date && /^(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/.test(String(patchIn.date))) patch.date = String(patchIn.date);
    if(patchIn.note){ patch.note = String(patchIn.note).trim().slice(0,500); }
    if(patchIn.categoryName){ patch.categoryName = String(patchIn.categoryName).trim().slice(0,80); }
    const type = String(patchIn.type||'').toLowerCase(); if(['income','expense'].includes(type)) patch.type = type;
    if(patchIn.currency){ patch.currency = String(patchIn.currency).toUpperCase(); }
    const cAmt = Number(patchIn.claimAmount); if(Number.isFinite(cAmt) && cAmt>=0) patch.claimAmount = cAmt;
    if(typeof patchIn.claimed==='boolean') patch.claimed = patchIn.claimed;
    if(Object.keys(criteria).length>0) out.criteria = criteria;
    if(Object.keys(patch).length>0) out.patch = patch;
    return out;
  }catch{ return null; }
}

async function findBestMatchingTransaction(userIdOrUid, criteria){
  try{
    const isDb = isDbEnabled();
    const txs = isDb ? await pgdb.getTransactions(userIdOrUid) : fileStore.getTransactions(userIdOrUid||'anonymous');
    const cats = isDb ? await pgdb.getCategories() : fileStore.getCategories(userIdOrUid||'anonymous');
    const idToName = new Map(cats.map(c=>[c.id, c.name]));
    let candidates = txs.slice(0,1000);
    const noteContains = String(criteria?.noteContains||'').trim();
    if(criteria?.date){
      const now = new Date();
      let ymd='';
      if(/^(\d{1,2})\/(\d{1,2})$/.test(criteria.date)){
        const m = criteria.date.match(/^(\d{1,2})\/(\d{1,2})$/);
        ymd = `${now.getFullYear()}-${String(Number(m[1])).padStart(2,'0')}-${String(Number(m[2])).padStart(2,'0')}`;
      }else if(/今天|昨天|前天/.test(criteria.date)){
        const d = new Date();
        if(/昨天/.test(criteria.date)) d.setDate(d.getDate()-1);
        if(/前天/.test(criteria.date)) d.setDate(d.getDate()-2);
        ymd = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
      }else if(/^20\d{2}-\d{2}-\d{2}$/.test(criteria.date)){
        ymd = criteria.date;
      }
      if(ymd) candidates = candidates.filter(t=> String(t.date)===ymd);
    }
    if(Number.isFinite(Number(criteria?.amount))){
      const a = Number(criteria.amount);
      candidates = candidates.filter(t=> Number(t.amount)===a);
    }
    if(noteContains){
      const low = noteContains.toLowerCase();
      candidates = candidates.filter(t=> String(t.note||'').toLowerCase().includes(low));
    }
    if(criteria?.categoryName){
      const low = String(criteria.categoryName).toLowerCase();
      candidates = candidates.filter(t=> String(idToName.get(t.categoryId)||'').toLowerCase()===low);
    }
    // pick most recent
    candidates.sort((a,b)=> (String(b.date).localeCompare(String(a.date))) || (String(b.id).localeCompare(String(a.id))));
    return candidates[0] || null;
  }catch{ return null; }
}

function todayYmd(){ const d=new Date(); return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`; }

async function handleStatsQuery(userId, text){
  const lower = String(text||'').toLowerCase();
  const txs = await (isDbEnabled() ? pgdb.getTransactions(userId) : fileStore.getTransactions(userId||'anonymous'));
  const ym = `${new Date().getFullYear()}-${String(new Date().getMonth()+1).padStart(2,'0')}`;
  const monthTx = txs.filter(t=> (t.date||'').startsWith(ym));
  const sum = (arr)=> arr.reduce((s,t)=> s + ((Number(t.amount)||0) * (t.type==='income'?1:-1)), 0);
  if(/(本月|這月).*(總)?支出/.test(text)){
    const expense = monthTx.filter(t=>t.type==='expense').reduce((s,t)=> s + (Number(t.amount)||0), 0);
    return `本月支出共 $${expense.toFixed(2)}`;
  }
  const catM = text.match(/(本月|這月).*?(餐飲|交通|購物|薪資)/);
  if(catM){
    const name = catM[2];
    const cats = await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(userId||'anonymous'));
    const hit = cats.find(c=> c.name===name);
    const used = monthTx.filter(t=> t.type==='expense' && (t.categoryId===hit?.id || t.categoryName===name));
    const val = used.reduce((s,t)=> s + (Number(t.amount)||0), 0);
    return `本月「${name}」支出 $${val.toFixed(2)}`;
  }
  if(/(查帳|統計|餘額)/.test(text)){
    const income = monthTx.filter(t=>t.type==='income').reduce((s,t)=> s + (Number(t.amount)||0), 0);
    const expense = monthTx.filter(t=>t.type==='expense').reduce((s,t)=> s + (Number(t.amount)||0), 0);
    return `本月收入 $${income.toFixed(2)}、支出 $${expense.toFixed(2)}、結餘 $${(income-expense).toFixed(2)}`;
  }
  return '';
}

const server = http.createServer(async (req, res) => {
  try {
    setCors(res);
    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      return res.end();
    }

    const url = new URL(req.url || '/', `http://${req.headers.host}`);
    const reqPath = url.pathname;
    const normPath = String(reqPath || '').replace(/\/+$/,'').toLowerCase();
    try{ console.log('[req]', req.method, reqPath); }catch(_){ }

    // 若允許匿名，為未登入使用者建立持久化 uid cookie
    ensureAnonCookie(req, res);

    // LINE Login start
    if (req.method === 'GET' && reqPath === '/auth/line/start'){
      // guard: required envs
      if(!LINE_LOGIN_CHANNEL_ID || !LINE_LOGIN_CHANNEL_SECRET || !LINE_LOGIN_REDIRECT_URI){
        res.writeHead(500, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:false, error:'line_login_env', missing: {
          LINE_LOGIN_CHANNEL_ID: !LINE_LOGIN_CHANNEL_ID,
          LINE_LOGIN_CHANNEL_SECRET: !LINE_LOGIN_CHANNEL_SECRET,
          LINE_LOGIN_REDIRECT_URI: !LINE_LOGIN_REDIRECT_URI
        }}));
      }
      const nonce = crypto.randomBytes(12).toString('hex');
      const random = crypto.randomBytes(12).toString('hex');
      const linkCode = url.searchParams.get('link')||'';
      const statePayload = linkCode ? `${random}|${nonce}|${Date.now()}|link=${linkCode}` : `${random}|${nonce}|${Date.now()}`;
      const signedState = createSigned(statePayload);
      const authz = new URL('https://access.line.me/oauth2/v2.1/authorize');
      authz.searchParams.set('response_type','code');
      authz.searchParams.set('client_id', LINE_LOGIN_CHANNEL_ID);
      authz.searchParams.set('redirect_uri', LINE_LOGIN_REDIRECT_URI);
      authz.searchParams.set('state', signedState);
      authz.searchParams.set('scope','profile openid');
      authz.searchParams.set('nonce', nonce);
      try{ console.log('[line-login] authorize URL', authz.toString()); }catch(_){ }
      res.writeHead(302, { Location: authz.toString() });
      return res.end();
    }
    // LINE Login callback
    if (req.method === 'GET' && reqPath === '/auth/line/callback'){
      const code = url.searchParams.get('code')||'';
      const state = url.searchParams.get('state')||'';
      const parsed = verifySigned(state);
      if(!parsed){ res.writeHead(400, { 'Content-Type':'text/plain; charset=utf-8' }); return res.end('Invalid state'); }
      if(!code){ res.writeHead(400, { 'Content-Type':'text/plain; charset=utf-8' }); return res.end('Missing code'); }
      // exchange token
      const tokenEndpoint = 'https://api.line.me/oauth2/v2.1/token';
      const form = new URLSearchParams();
      form.set('grant_type','authorization_code');
      form.set('code', code);
      form.set('redirect_uri', LINE_LOGIN_REDIRECT_URI);
      form.set('client_id', LINE_LOGIN_CHANNEL_ID);
      form.set('client_secret', LINE_LOGIN_CHANNEL_SECRET);
      let tokenData=null;
      try{
        tokenData = await fetchJson(tokenEndpoint, { method:'POST', headers:{ 'Content-Type':'application/x-www-form-urlencoded' }, body: String(form) }, 15000);
      }catch(err){
        try{ console.error('[line-login] token error', err); }catch(_){ }
        res.writeHead(502, { 'Content-Type':'text/plain; charset=utf-8' }); return res.end('Token exchange failed');
      }
      const accessToken = tokenData?.access_token||'';
      if(!accessToken){ res.writeHead(502, { 'Content-Type':'text/plain; charset=utf-8' }); return res.end('Missing access token'); }
      // fetch profile
      let profile=null;
      try{
        profile = await fetchJson('https://api.line.me/v2/profile', { headers:{ 'Authorization': `Bearer ${accessToken}` } }, 15000);
      }catch(err){ try{ console.error('[line-login] profile error', err); }catch(_){ } profile=null; }
      const user = {
        id: profile?.userId ? `line:${profile.userId}` : ('line:'+(tokenData?.id_token||'').slice(0,8)),
        name: profile?.displayName || 'LINE User',
        picture: profile?.pictureUrl || ''
      };
      // Stateless cookie: store signed uid directly
      const isHttps = /^https:\/\//.test(getBaseUrl(req)||'');
      setCookie(res, 'session', createSigned(`uid:${user.id}`), { maxAge: 60*60*24*7, secure: isHttps });
      // Consume link code (if present in state) to bind LINE bot user to this web account
      try{
        const parts = parsed.split('|');
        const pair = parts.find(p=> p && p.startsWith('link='));
        if(pair){
          const codeStr = pair.slice('link='.length);
          if(isDbEnabled()){
            const lineUid = await pgdb.consumeLinkCode(codeStr);
            if(lineUid){ await pgdb.upsertLink(lineUid, user.id); }
          } else {
            const lineUid = linkStore.consumeLinkCode(codeStr);
            if(lineUid){
              linkStore.upsertLink(lineUid, user.id);
              // move existing local data from LINE user bucket to web user bucket if needed
              try{ fileStore.migrateUserData(lineUid, user.id); }catch(_){ }
            }
          }
        }
      }catch(_){ }
      // File-based migration: move from legacy IDs to new LINE-based ID if present
      try{
        if(!isDbEnabled()){
          // common legacy IDs: raw profile userId (without prefix) and anonymous
          const legacyRaw = (profile?.userId)||'';
          if(legacyRaw){ try{ fileStore.migrateUserData(legacyRaw, user.id); }catch(_){ } }
          try{ fileStore.migrateUserData('anonymous', user.id); }catch(_){ }
          // 若先前以匿名 uid 使用，將匿名資料搬移到登入帳號
          try{
            const cookies = parseCookies(req);
            const anon = verifySigned(cookies['uid']||'');
            if(anon){ fileStore.migrateUserData(anon, user.id); }
          }catch(_){ }
        }
      }catch(_){ }
      res.writeHead(302, { Location: '/' });
      return res.end();
    }
    // get current user
    if (req.method === 'GET' && reqPath === '/api/me'){
      const user = getUserFromRequest(req);
      if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); }
      res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify({ ok:true, user }));
    }
    // logout
    if (req.method === 'POST' && reqPath === '/auth/logout'){
      // Stateless: clear cookie only; legacy map deletion is best-effort
      const cookies = parseCookies(req);
      const raw = cookies['session']||'';
      const signed = verifySigned(raw);
      if(signed && !String(signed).startsWith('uid:')){ sessions.delete(signed); }
      clearCookie(res, 'session');
      res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify({ ok:true }));
    }
    if (req.method === 'POST' && reqPath === '/api/ai') {
      if(REQUIRE_AUTH){ const user = getUserFromRequest(req); if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false, error:'unauthorized' })); } }
      const raw = await parseBody(req);
      const body = JSON.parse(raw.toString('utf-8') || '{}');
      const { messages = [], context = {}, mode = 'chat' } = body || {};

      const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';
      const OPENAI_BASE_URL = process.env.OPENAI_BASE_URL || 'https://api.openai.com/v1';
      const OPENAI_MODEL = process.env.OPENAI_MODEL || 'gpt-4o-mini';

      // Fallback: simple heuristic response if no key configured
      if (!OPENAI_API_KEY) {
        const reply = heuristicReply(messages, context);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok: true, provider: 'heuristic', reply }));
      }

      // Proxy to OpenAI-compatible API
      const payload = {
        model: OPENAI_MODEL,
        messages: (mode==='struct') ? [
          { role: 'system', content: `你是一個記帳解析器，請輸出 JSON，包含: type(income|expense), amount(number), currency(string), date(YYYY-MM-DD；若為 MM/DD 則年份取今年；支援 今天/昨天/前天/明天), categoryName(string), rate(number，可省略), claimAmount(number，可省略), claimed(boolean，可省略), note(string)。金額可含中文數字。請優先從提供的分類清單選擇 categoryName，對不到可輸出新名稱，前端會自動建立。${Array.isArray(context?.categories)?'分類清單：'+context.categories.map(c=>c.name).join(', ').slice(0,800):''} 嚴禁將日期/時間/序數視為金額（例如：十月、十點、10:30、10/31、第三次）。若不確定金額，省略 amount 欄位，切勿臆測。只輸出 JSON，不要其他文字。` },
          { role: 'user', content: messages?.[0]?.content || '' }
        ] : [
          { role: 'system', content: 'You are a helpful finance and budgeting assistant for a personal ledger web app. Answer in Traditional Chinese.' },
          { role: 'system', content: `Context JSON (may be partial): ${JSON.stringify(context).slice(0, 4000)}` },
          ...messages
        ],
        temperature: (mode==='struct') ? 0 : 0.4
      };
      const base = (OPENAI_BASE_URL || '').replace(/\/+$/,'');
      const apiBase = /\/v\d+(?:$|\/)/.test(base) ? base : `${base}/v1`;
      const endpoint = `${apiBase}/chat/completions`;
      try{
        const data = await fetchJson(endpoint, {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${OPENAI_API_KEY}`,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(payload)
        }, 20000);
        const reply = data?.choices?.[0]?.message?.content || '';
        // struct 模式：嘗試解析 JSON
        if(mode==='struct'){
          try{
            const raw = JSON.parse(reply);
            const normalized = validateAndNormalizeStruct(raw);
            const merged = mergeParsedAmountFromText(messages?.[0]?.content||'', normalized);
            res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
            return res.end(JSON.stringify({ ok:true, provider:'openai', parsed: merged }));
          }catch(_){ /* fallthrough to plain text */ }
        }
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok: true, provider: 'openai', reply }));
      }catch(err){
        res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:false, error:'ai_provider', detail: String(err?.message||err) }));
      }
    }

    // AI streaming (SSE)
    if (req.method === 'POST' && reqPath === '/api/ai/stream') {
      if(REQUIRE_AUTH){ const user = getUserFromRequest(req); if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false, error:'unauthorized' })); } }
      const raw = await parseBody(req);
      const body = JSON.parse(raw.toString('utf-8') || '{}');
      const { messages = [], context = {} } = body || {};

      const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';
      const OPENAI_BASE_URL = process.env.OPENAI_BASE_URL || 'https://api.openai.com/v1';
      const OPENAI_MODEL = process.env.OPENAI_MODEL || 'gpt-4o-mini';

      // Prepare SSE response
      res.writeHead(200, {
        'Content-Type': 'text/event-stream; charset=utf-8',
        'Cache-Control': 'no-cache, no-transform',
        'Connection': 'keep-alive'
      });
      const sse = (obj)=>{ try{ res.write(`data: ${JSON.stringify(obj)}\n\n`); }catch(_){ } };
      let closed = false;
      req.on('close', ()=>{ closed = true; try{ res.end(); }catch(_){ } });

      // Fallback streaming when no key: stream heuristic reply in chunks
      if(!OPENAI_API_KEY){
        try{
          const reply = heuristicReply(messages, context) || '（離線模式）';
          const parts = String(reply).split(/(\s+)/).filter(Boolean);
          for(const p of parts){ if(closed) break; sse({ delta: p }); await new Promise(r=>setTimeout(r, 20)); }
          if(!closed) sse({ done:true });
        }catch(_){ try{ sse({ error:'stream_offline_failed' }); }catch(_){ } }
        return; // keep connection open until client closes
      }

      // Upstream OpenAI-compatible streaming
      try{
        const base = (OPENAI_BASE_URL||'').replace(/\/+$/,'');
        const apiBase = /\/v\d+(?:$|\/)/.test(base) ? base : `${base}/v1`;
        const endpoint = new URL(`${apiBase}/chat/completions`);
        const payload = {
          model: OPENAI_MODEL,
          messages: [
            { role: 'system', content: 'You are a helpful finance and budgeting assistant for a personal ledger web app. Answer in Traditional Chinese.' },
            { role: 'system', content: `Context JSON (may be partial): ${JSON.stringify(context).slice(0, 4000)}` },
            ...messages
          ],
          temperature: 0.4,
          stream: true
        };
        const reqUp = https.request({
          hostname: endpoint.hostname,
          path: endpoint.pathname + endpoint.search,
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${OPENAI_API_KEY}`,
            'Content-Type': 'application/json'
          }
        }, (resp)=>{
          resp.setEncoding('utf8');
          let buffer = '';
          resp.on('data', (chunk)=>{
            buffer += chunk;
            const lines = buffer.split(/\n/);
            buffer = lines.pop()||'';
            for(const line of lines){
              const trimmed = line.trim();
              if(!trimmed) continue;
              if(trimmed.startsWith('data:')){
                const data = trimmed.slice(5).trim();
                if(data === '[DONE]'){ sse({ done:true }); return; }
                try{
                  const json = JSON.parse(data);
                  const delta = json?.choices?.[0]?.delta?.content || '';
                  if(delta){ sse({ delta }); }
                }catch(_){ /* ignore */ }
              }
            }
          });
          resp.on('end', ()=>{ try{ sse({ done:true }); }catch(_){ } });
        });
        reqUp.on('error', ()=>{ try{ sse({ error:'upstream_error' }); }catch(_){ } });
        reqUp.write(JSON.stringify(payload));
        reqUp.end();
      }catch(_){ try{ sse({ error:'stream_init_failed' }); }catch(_){ } }

      return; // keep SSE open
    }

    if (req.method === 'GET' && reqPath === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      return res.end(JSON.stringify({ ok: true }));
    }

    // Admin broadcast (protected by LINE admin user id in cookie-mapped session OR query token)
    if (req.method === 'POST' && reqPath === '/api/admin/broadcast'){
      const raw = await parseBody(req);
      const body = JSON.parse(raw.toString('utf-8')||'{}');
      const { message='' } = body||{};
      // Admin check: if logged-in LINE account equals admin line user id OR if X-Admin-Key matches env secret
      const ADMIN_KEY = process.env.ADMIN_KEY || '';
      const user = getUserFromRequest(req);
      const isAdminByKey = ADMIN_KEY && (String(req.headers['x-admin-key']||'')===ADMIN_KEY);
      let isAdminByLine = false;
      try{
        // find if current session is a LINE login with admin id
        // we do not store raw line id in session, so fallback: allow anonymous by header key only
        const cookies = parseCookies(req);
        const sid = verifySigned(cookies['session']||'');
        if(sid){
          const s = sessions.get(sid);
          const uid = s?.user?.id||'';
          if(uid && uid.startsWith('line:')){
            const raw = uid.slice('line:'.length);
            isAdminByLine = (raw===ADMIN_LINE_USER_ID);
          }
        }
      }catch(_){ }
      if(!(isAdminByKey || isAdminByLine)){
        res.writeHead(403, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:false, error:'forbidden' }));
      }
      if(!String(message).trim()){
        res.writeHead(400, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:false, error:'empty_message' }));
      }
      // Push to all known LINE user ids (linked users). If DB disabled, load from shared links file.
      let targets = [];
      try{
        if(isDbEnabled()){
          const ids = await pgdb.listAllLineUserIds();
          targets = Array.isArray(ids)?ids:[];
        }else{
          const map = readJson(sharedPath('links.json'), {});
          targets = Object.keys(map||{});
        }
      }catch(_){ targets = []; }
      // Fallback: if no targets, but admin id exists, at least send to admin
      if(targets.length===0 && ADMIN_LINE_USER_ID){ targets=[ADMIN_LINE_USER_ID]; }
      const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req), title:'系統公告', subtitle: new Date().toLocaleString('zh-TW'), lines:[ String(message) ], buttons:[ { style:'link', action:{ type:'uri', label:'開啟網頁版', uri: getBaseUrl(req)||'https://example.com' } } ], showHero:false, compact:true });
      let success=0;
      for(const to of targets){
        const ok = await linePush(to, [{ type:'flex', altText:'系統公告', contents:bubble }]);
        if(ok) success++;
      }
      // Store latest notice for frontend fetch
      try{
        const data = readJson(sharedPath('notice.json'), {});
        data.latest = { message:String(message), date: new Date().toISOString() };
        writeJson(sharedPath('notice.json'), data);
      }catch(_){ }
      res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify({ ok:true, sent: success, total: targets.length }));
    }

    // Public endpoint: get latest notice (for web UI)
    if (req.method === 'GET' && reqPath === '/api/notice/latest'){
      try{
        const data = readJson(sharedPath('notice.json'), {});
        res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, latest: data.latest||null }));
      }catch(_){
        res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, latest:null }));
      }
    }

    // Admin info: whether current session is admin (for frontend UI gating)
    if (req.method === 'GET' && reqPath === '/api/admin/info'){
      const ADMIN_KEY = process.env.ADMIN_KEY || '';
      const user = getUserFromRequest(req);
      let isAdminByLine = false;
      try{
        const cookies = parseCookies(req);
        const sid = verifySigned(cookies['session']||'');
        if(sid){
          const s = sessions.get(sid);
          const uid = s?.user?.id||'';
          if(uid && uid.startsWith('line:')){
            const raw = uid.slice('line:'.length);
            isAdminByLine = (raw===ADMIN_LINE_USER_ID);
          }
        }
      }catch(_){ }
      const hasKey = ADMIN_KEY && String(req.headers['x-admin-key']||'')===ADMIN_KEY;
      const admin = Boolean(isAdminByLine || hasKey);
      res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify({ ok:true, admin }));
    }

    // Serve static frontend (index.html, styles.css, app.js, db.js) from project root
    if (req.method === 'GET' && (reqPath === '/' || reqPath === '/index.html' || reqPath === '/styles.css' || reqPath === '/app.js' || reqPath === '/db.js' || reqPath === '/flex-glass.svg')) {
      const __filename = fileURLToPath(import.meta.url);
      const __dirname = path.dirname(__filename);
      const rootDir = path.resolve(__dirname, '..');
      const filePath = reqPath === '/' ? path.join(rootDir, 'index.html') : path.join(rootDir, reqPath.slice(1));
      try{
        const ext = reqPath === '/' ? '.html' : path.extname(filePath);
        const mime = ext === '.html' ? 'text/html; charset=utf-8'
          : ext === '.css' ? 'text/css; charset=utf-8'
          : ext === '.js' ? 'application/javascript; charset=utf-8'
          : ext === '.svg' ? 'image/svg+xml; charset=utf-8'
          : 'text/plain; charset=utf-8';
        const content = fs.readFileSync(filePath);
        res.writeHead(200, { 'Content-Type': mime });
        return res.end(content);
      }catch(_){ /* fallthrough to API/404 */ }
    }

    // Categories
    if (req.method === 'GET' && reqPath === '/api/categories') {
      if(REQUIRE_AUTH){ const user = getUserFromRequest(req); if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify([])); } }
      const user = REQUIRE_AUTH ? getUserFromRequest(req) : null;
      if(isDbEnabled()){
        const rows = await pgdb.getCategories();
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(rows));
      } else {
        const uid = reqUserId(req);
        const rows = fileStore.getCategories(uid);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(rows));
      }
    }
    if (req.method === 'POST' && reqPath === '/api/categories') {
      if(REQUIRE_AUTH){ const user = getUserFromRequest(req); if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false, error:'unauthorized' })); } }
      const raw = await parseBody(req);
      const { name = '' } = JSON.parse(raw.toString('utf-8') || '{}');
      const user = REQUIRE_AUTH ? getUserFromRequest(req) : null;
      if(isDbEnabled()){
        if(!String(name).trim()){ res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false, error:'invalid_name' })); }
        const cat = await pgdb.addCategory(name);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, category: cat }));
      } else {
        const uid = reqUserId(req);
        const cat = fileStore.addCategory(uid, name);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok: true, category: cat }));
      }
    }
    if (req.method === 'DELETE' && reqPath.startsWith('/api/categories/')){
      if(REQUIRE_AUTH){ const user = getUserFromRequest(req); if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false, error:'unauthorized' })); } }
      const id = decodeURIComponent(reqPath.split('/').pop()||'');
      const user = REQUIRE_AUTH ? getUserFromRequest(req) : null;
      if(isDbEnabled()){
        const ok = await pgdb.deleteCategory(id);
        if(!ok){ res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false, error:'in_use' })); }
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true }));
      } else {
        const uid = reqUserId(req);
        const ok = fileStore.deleteCategory(uid, id);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok }));
      }
    }

    // Transactions
    if (req.method === 'GET' && reqPath === '/api/transactions'){
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify([])); } }
      const user = getUserFromRequest(req);
      if(isDbEnabled()){
        const rows = await pgdb.getTransactions(user?.id);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(rows));
      } else {
        const uid = user?.id || reqUserId(req);
        const rows = fileStore.getTransactions(uid);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(rows));
      }
    }
    if (req.method === 'POST' && reqPath === '/api/transactions'){
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false, error:'unauthorized' })); } }
      const raw = await parseBody(req);
      const payload = JSON.parse(raw.toString('utf-8') || '{}');
      const user = getUserFromRequest(req);
      if(isDbEnabled()){
        const rec = await pgdb.addTransaction(user?.id, payload);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      } else {
        const uid = user?.id || reqUserId(req);
        const rec = fileStore.addTransaction(uid, payload);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      }
    }
    if (req.method === 'GET' && reqPath.startsWith('/api/transactions/')){
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const id = decodeURIComponent(reqPath.split('/').pop()||'');
      const user = getUserFromRequest(req);
      if(isDbEnabled()){
        const rec = await pgdb.getTransactionById(user?.id, id);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      } else {
        const uid = user?.id || reqUserId(req);
        const rec = fileStore.getTransactionById(uid, id);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      }
    }
    if (req.method === 'PUT' && reqPath.startsWith('/api/transactions/')){
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const id = decodeURIComponent(reqPath.split('/').pop()||'');
      const raw = await parseBody(req);
      const patch = JSON.parse(raw.toString('utf-8') || '{}');
      const user = getUserFromRequest(req);
      if(isDbEnabled()){
        const rec = await pgdb.updateTransaction(user?.id, id, patch);
        if(!rec){ res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); }
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      } else {
        const uid = user?.id || reqUserId(req);
        const rec = fileStore.updateTransaction(uid, id, patch);
        if(!rec){ res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); }
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      }
    }
    if (req.method === 'DELETE' && reqPath.startsWith('/api/transactions/')){
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const id = decodeURIComponent(reqPath.split('/').pop()||'');
      const user = getUserFromRequest(req);
      if(isDbEnabled()){
        await pgdb.deleteTransaction(user?.id, id);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true }));
      } else {
        const uid = user?.id || reqUserId(req);
        const ok = fileStore.deleteTransaction(uid, id);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok }));
      }
    }

    // Settings
    if (req.method === 'GET' && reqPath === '/api/settings'){
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({})); } }
      const user = getUserFromRequest(req);
      if(isDbEnabled()){
        const s = await pgdb.getSettings(user?.id);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(s));
      } else {
        const uid = user?.id || reqUserId(req);
        const s = fileStore.getSettings(uid);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(s));
      }
    }
    if (req.method === 'PUT' && reqPath === '/api/settings'){
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({})); } }
      const raw = await parseBody(req);
      const patch = JSON.parse(raw.toString('utf-8') || '{}');
      const user = getUserFromRequest(req);
      if(isDbEnabled()){
        const next = await pgdb.setSettings(user?.id, patch);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(next));
      } else {
        const uid = user?.id || reqUserId(req);
        const next = fileStore.setSettings(uid, patch);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(next));
      }
    }

    // Model
    if (req.method === 'POST' && reqPath === '/api/model/update'){
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const raw = await parseBody(req);
      const { note='', categoryId='' } = JSON.parse(raw.toString('utf-8')||'{}');
      const user = getUserFromRequest(req);
      if(isDbEnabled()){
        await pgdb.updateCategoryModelFromNote(user?.id, note, categoryId);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true }));
      } else {
        const uid = user?.id || reqUserId(req);
        await fileStore.updateCategoryModelFromNote(uid, note, categoryId);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true }));
      }
    }
    if (req.method === 'POST' && reqPath === '/api/model/suggest'){
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const raw = await parseBody(req);
      const { note='' } = JSON.parse(raw.toString('utf-8')||'{}');
      const user = getUserFromRequest(req);
      if(isDbEnabled()){
        const catId = await pgdb.suggestCategoryFromNote(user?.id, note);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, categoryId: catId }));
      } else {
        const uid = user?.id || reqUserId(req);
        const best = fileStore.suggestCategoryFromNote(uid, note);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, categoryId: best }));
      }
    }

    // Model backfill: train from all existing transactions (admin or authenticated user only)
    if (req.method === 'POST' && reqPath === '/api/model/backfill'){
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const user = getUserFromRequest(req);
      try{
        if(isDbEnabled()){
          const rows = await pgdb.getTransactions(user?.id);
          for(const t of rows){ if(t && t.note && t.categoryId){ await pgdb.updateCategoryModelFromNote(user?.id, t.note, t.categoryId); } }
        }else{
          const uid = user?.id || 'anonymous';
          const rows = fileStore.getTransactions(uid);
          for(const t of rows){ if(t && t.note && t.categoryId){ await fileStore.updateCategoryModelFromNote(uid, t.note, t.categoryId); } }
        }
        res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true }));
      }catch(err){
        res.writeHead(500, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:false }));
      }
    }

    // Model inspect: show current model keywords/weights (limited)
    if (req.method === 'GET' && reqPath === '/api/model/inspect'){
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const user = getUserFromRequest(req);
      try{
        if(isDbEnabled()){
          const rows = await pgdb._getAllModel(user?.id);
          res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
          return res.end(JSON.stringify({ ok:true, model: rows.slice(0,500) }));
        }else{
          const uid = user?.id || 'anonymous';
          const rows = (function(){ try{ return require('fs').existsSync ? [] : []; }catch(_){ return []; } })();
          // fallback: we do not persist a separate model file listing here; expose none for fileStore
          res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
          return res.end(JSON.stringify({ ok:true, model: [] }));
        }
      }catch(err){
        res.writeHead(500, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:false }));
      }
    }

    if (req.method === 'GET' && reqPath === '/api/sync/export') {
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const user = getUserFromRequest(req);
      if(isDbEnabled()){
        const data = await pgdb.exportAll(user?.id);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(data));
      } else {
        const uid = user?.id || reqUserId(req);
        const filtered = fileStore.exportAll(uid);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(filtered));
      }
    }

    if (req.method === 'POST' && reqPath === '/api/sync/import') {
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const raw = await parseBody(req);
      const data = JSON.parse(raw.toString('utf-8') || '{}');
      if (!data || !Array.isArray(data.categories) || !Array.isArray(data.transactions)) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok: false, error: 'invalid payload' }));
      }
      const user = getUserFromRequest(req);
      if(isDbEnabled()){
        await pgdb.importAll(user?.id, data);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok: true }));
      } else {
        const uid = user?.id || reqUserId(req);
        fileStore.importAll(uid, data);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok: true }));
      }
    }

    const isLineWebhookPath = (normPath === '/line/webhook' || normPath.startsWith('/line/webhook/'));
    // Some platforms (and LINE verify button) may send a GET to verify availability
    if (req.method === 'GET' && isLineWebhookPath){
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      return res.end(JSON.stringify({ ok:true, method:'GET' }));
    }
    if (req.method === 'POST' && isLineWebhookPath) {
      const raw = await parseBody(req);
      const sig = req.headers['x-line-signature'];
      try{ console.log('[line] webhook hit', { method:req.method, sig: !!sig, rawBytes: raw?.length||0 }); }catch(_){ }
      if (!verifyLineSignature(raw, typeof sig === 'string' ? sig : '')) {
        res.writeHead(401, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok: false, error: 'signature invalid' }));
      }
      let body = {};
      try { body = JSON.parse(raw.toString('utf-8') || '{}'); } catch {}
      const events = Array.isArray(body.events) ? body.events : [];
      // Handle message events
      for(const ev of events){
        try{
          try{ console.log('[line] event', { type: ev.type, msg: ev.message?.text, hasToken: !!LINE_CHANNEL_ACCESS_TOKEN, db: isDbEnabled() }); }catch(_){ }
          // Follow event: send binding Flex message
          if(ev.type==='follow'){
            const lineUidRaw = ev.source?.userId || '';
            if(lineUidRaw){
              const base = getBaseUrl(req) || '';
              if(isDbEnabled()){
                const code = await pgdb.createLinkCode(`line:${lineUidRaw}`, 900);
                const linkUrl = `${base}/auth/line/start?link=${encodeURIComponent(code)}`;
                const bubble = glassFlexBubble({ baseUrl: base, title: '綁定帳號', subtitle: '登入網站即可完成綁定', lines:[ '完成後可直接用 LINE 記帳與查詢統計。' ], buttons:[ { style:'primary', action:{ type:'uri', label:'前往綁定', uri: linkUrl } }, { style:'link', action:{ type:'uri', label:'瞭解更多', uri: base||'https://example.com' } } ] });
                await lineReply(ev.replyToken, [{ type:'flex', altText:'綁定帳號', contents:bubble }]);
              } else {
                const code = linkStore.createLinkCode(`line:${lineUidRaw}`, 900);
                if(code){
                  const linkUrl = `${base}/auth/line/start?link=${encodeURIComponent(code)}`;
                  const bubble = glassFlexBubble({ baseUrl: base, title: '綁定帳號', subtitle: '登入網站即可完成綁定', lines:[ '完成後可直接用 LINE 記帳與查詢統計。' ], buttons:[ { style:'primary', action:{ type:'uri', label:'前往綁定', uri: linkUrl } } ] });
                  await lineReply(ev.replyToken, [{ type:'flex', altText:'綁定帳號', contents:bubble }]);
                }
              }
            }
            continue;
          }
          // Postback events (for guided flow and menu)
          if(ev.type==='postback'){
            const replyToken = ev.replyToken;
            const lineUidRaw = ev.source?.userId || '';
            let userId = lineUidRaw ? `line:${lineUidRaw}` : null;
            try{
              if(lineUidRaw){
                if(isDbEnabled()){
                  const mapped = await pgdb.getLinkedWebUser(`line:${lineUidRaw}`);
                  if(mapped) userId = mapped;
                } else {
                  const mapped = linkStore.getLinkedWebUser(`line:${lineUidRaw}`);
                  if(mapped) userId = mapped;
                }
              }
            }catch(_){ }
            const dataObj = parsePostbackData(ev.postback?.data||'');
            const flow = dataObj.flow;
            const base = getBaseUrl(req)||'';
            // Feature flow handlers (demo for OpenAI Responses capability fallbacks)
            if(flow==='feature'){
              const step = String(dataObj.step||'');
              if(step==='responses_text'){
                const text = '生成一段關於彩虹獨角獸的一句短篇故事。';
                const reply = await aiChatText(text, { demo:true });
                await lineReply(replyToken, [{ type:'text', text: reply.slice(0,1000) }]);
                continue;
              }
              if(step==='vision'){
                const info = '圖片理解示範：目前未上傳圖片，請直接貼圖片網址與問題，我會嘗試描述內容。';
                await lineReply(replyToken, [{ type:'text', text: info }]);
                continue;
              }
              if(step==='web_search'){
                const info = '網頁搜尋示範：目前未接上外部搜尋服務。你可以先告訴我關鍵字，我用一般對話先提供方向。';
                await lineReply(replyToken, [{ type:'text', text: info }]);
                continue;
              }
              if(step==='file_search'){
                const info = '文件搜尋示範：目前未接上向量資料庫。你可先上傳文字檔並詢問重點，我用一般對話回覆摘要。';
                await lineReply(replyToken, [{ type:'text', text: info }]);
                continue;
              }
              if(step==='function_tools'){
                const info = '函式工具示範：你可以說「查台北天氣」，我會嘗試以內建邏輯回覆（暫無外部 API）。';
                await lineReply(replyToken, [{ type:'text', text: info }]);
                continue;
              }
              if(step==='stream'){
                const info = '串流示範：目前 LINE 不支援逐字串流顯示，我會在後端完整生成後一次回覆。';
                await lineReply(replyToken, [{ type:'text', text: info }]);
                continue;
              }
            }
            // Admin actions
            if(flow==='admin'){
              const replyToken = ev.replyToken;
              const lineUidRaw = ev.source?.userId || '';
              if(!isLineAdmin(lineUidRaw)){
                const bubble = glassFlexBubble({ baseUrl:base, title:'權限不足', subtitle:'僅管理者可用', lines:[], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'權限不足', contents:bubble }]);
                continue;
              }
              const step = String(dataObj.step||'');
              if(step==='cancel'){
                adminState.delete(`line:${lineUidRaw}`);
                const bubble = glassFlexBubble({ baseUrl:base, title:'已關閉', subtitle:'', lines:[], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'已關閉', contents:bubble }]);
                continue;
              }
              if(step==='broadcast'){
                adminState.set(`line:${lineUidRaw}`, { mode:'broadcast_wait' });
                const bubble = glassFlexBubble({ baseUrl:base, title:'輸入廣播內容', subtitle:'請直接輸入文字訊息', lines:[], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'輸入廣播內容', contents:bubble }]);
                continue;
              }
              if(step==='backfill'){
                // trigger backfill
                try{
                  if(isDbEnabled()){
                    const user = getUserFromRequest(req);
                    const rows = await pgdb.getTransactions(user?.id);
                    for(const t of rows){ if(t && t.note && t.categoryId){ await pgdb.updateCategoryModelFromNote(user?.id, t.note, t.categoryId); } }
                  }else{
                    // For non-auth: run global backfill routine
                    await backfillAllUsers();
                  }
                  const bubble = glassFlexBubble({ baseUrl:base, title:'回填已啟動', subtitle:'數分鐘內完成', lines:[], showHero:false, compact:true });
                  await lineReply(replyToken, [{ type:'flex', altText:'回填已啟動', contents:bubble }]);
                }catch(_){
                  const bubble = glassFlexBubble({ baseUrl:base, title:'回填失敗', subtitle:'請稍後再試', lines:[], showHero:false, compact:true });
                  await lineReply(replyToken, [{ type:'flex', altText:'回填失敗', contents:bubble }]);
                }
                continue;
              }
              if(step==='inspect'){
                try{
                  let lines=[];
                  if(isDbEnabled()){
                    const user = getUserFromRequest(req);
                    const rows = await pgdb._getAllModel(user?.id);
                    const top = rows.slice(0,10).map(r=> `${r.word}: ${Object.entries(r.counts||{}).slice(0,3).map(([k,v])=>`${k}:${v}`).join(' ')}`);
                    lines = top.length?top:['目前沒有模型資料'];
                  }else{
                    lines = ['檔案模式不提供模型摘要'];
                  }
                  const bubble = glassFlexBubble({ baseUrl:base, title:'模型摘要（前 10）', subtitle:new Date().toLocaleString('zh-TW'), lines, showHero:false, compact:true });
                  await lineReply(replyToken, [{ type:'flex', altText:'模型摘要', contents:bubble }]);
                }catch(_){
                  const bubble = glassFlexBubble({ baseUrl:base, title:'讀取失敗', subtitle:'請稍後再試', lines:[], showHero:false, compact:true });
                  await lineReply(replyToken, [{ type:'flex', altText:'讀取失敗', contents:bubble }]);
                }
                continue;
              }
            }
            // AI contextual postbacks
            if(flow==='ai'){
              const action = String(dataObj.action||'');
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              const rec = action ? aiPending.get(action) : null;
              // cancel
              if(dataObj.step==='cancel' && rec && rec.userId===uid){
                aiPending.delete(action);
                const bubble = glassFlexBubble({ baseUrl:base, title:'已取消', subtitle:'已中止動作', lines:[], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'已取消', contents:bubble }]);
                continue;
              }
              // confirm add
              if(dataObj.step==='add' && dataObj.do==='1' && rec && rec.userId===uid && rec.kind==='add_tx'){
                const payload = { ...rec.payload };
                // fallback category if empty
                if(!payload.categoryId){
                  try{
                    const cats = await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(uid));
                    payload.categoryId = cats[0]?.id || 'food';
                  }catch(_){ payload.categoryId = 'food'; }
                }
                let created=null;
                if(isDbEnabled()){
                  created = await pgdb.addTransaction(userId, payload);
                }else{
                  created = fileStore.addTransaction(uid, payload);
                }
                if(created?.id){ aiLastTxId.set(uid, created.id); }
                aiPending.delete(action);
                const bubble = glassFlexBubble({ baseUrl:base, title: payload.type==='income'?'已記收入':'已記支出', subtitle: payload.date, lines: summarizeTxLines({ ...payload, categoryId: payload.categoryId }, null), buttons:[ { style:'secondary', color:'#64748b', action:{ type:'postback', label:'編輯這筆', data:`flow=edit&step=amount&tx=${encodeURIComponent(created?.id||'')}` } } ], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'記帳完成', contents:bubble }]);
                continue;
              }
              // choose category → prompt
              if(dataObj.step==='choose_cat' && rec && rec.userId===uid && rec.kind==='add_tx'){
                const bubble = await buildAiChooseCategoryPrompt(base, isDbEnabled(), userId||uid, action);
                await lineReply(replyToken, [{ type:'flex', altText:'選擇分類', contents:bubble }]);
                continue;
              }
              // choose category do → add
              if(dataObj.step==='choose_cat_do' && dataObj.id && rec && rec.userId===uid && rec.kind==='add_tx'){
                const catId = String(dataObj.id);
                const payload = { ...rec.payload, categoryId: catId };
                let created=null;
                if(isDbEnabled()){
                  created = await pgdb.addTransaction(userId, payload);
                }else{
                  created = fileStore.addTransaction(uid, payload);
                }
                if(created?.id){ aiLastTxId.set(uid, created.id); }
                aiPending.delete(action);
                const bubble = glassFlexBubble({ baseUrl:base, title: payload.type==='income'?'已記收入':'已記支出', subtitle: payload.date, lines: summarizeTxLines(payload, null), buttons:[ { style:'secondary', color:'#64748b', action:{ type:'postback', label:'編輯這筆', data:`flow=edit&step=amount&tx=${encodeURIComponent(created?.id||'')}` } } ], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'記帳完成', contents:bubble }]);
                continue;
              }
              // confirm delete
              if(dataObj.step==='delete' && dataObj.do==='1' && rec && rec.userId===uid && rec.kind==='delete_tx'){
                const id = rec.txId || '';
                try{
                  if(isDbEnabled()) await pgdb.deleteTransaction(userId, id);
                  else fileStore.deleteTransaction(uid, id);
                }catch(_){ }
                aiPending.delete(action);
                const bubble = glassFlexBubble({ baseUrl:base, title:'已刪除', subtitle:'上一筆已刪除', lines:[], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'刪除完成', contents:bubble }]);
                continue;
              }
            }
            if(flow==='add'){
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              let state = guidedFlow.get(uid) || { step:'amount', payload:{ type:'expense', currency:'TWD', rate:1 } };
              if(dataObj.step==='start'){
                state = { step:'amount', payload:{ type:'expense', currency:'TWD', rate:1 } };
                guidedFlow.set(uid, state);
                const bubble = buildAmountPrompt(base);
                await lineReply(replyToken, [{ type:'flex', altText:'輸入金額', contents:bubble }]);
                continue;
              }
              if(dataObj.step==='cancel'){
                guidedFlow.delete(uid);
                const bubble = glassFlexBubble({ baseUrl:base, title:'已取消', subtitle:'已中止記帳流程', lines:[], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'已取消', contents:bubble }]);
                continue;
              }
              if(dataObj.step==='amount' && dataObj.value){
                const amt = Number(dataObj.value);
                if(Number.isFinite(amt) && amt>0){ state.payload.amount = amt; state.step='claim_ask'; guidedFlow.set(uid, state); }
                const bubble = buildClaimAskPrompt(base);
                await lineReply(replyToken, [{ type:'flex', altText:'選擇分類', contents:bubble }]);
                continue;
              }
              if(dataObj.step==='claim_ask'){
                const ans = String(dataObj.ans||'no');
                if(ans==='yes'){
                  state.step = 'claim_amount'; guidedFlow.set(uid, state);
                  const bubble = buildClaimAmountPrompt(base, state.payload.amount||0);
                  await lineReply(replyToken, [{ type:'flex', altText:'輸入請款金額', contents:bubble }]);
                  continue;
                }else{
                  state.payload.claimAmount = 0; state.payload.claimed = false; state.step='category'; guidedFlow.set(uid, state);
                  const bubble = await buildCategoryPrompt(base, isDbEnabled(), userId||uid);
                  await lineReply(replyToken, [{ type:'flex', altText:'選擇分類', contents:bubble }]);
                  continue;
                }
              }
              if(dataObj.step==='claim_amount' && dataObj.value){
                const v = Number(dataObj.value);
                if(Number.isFinite(v) && v>=0){ state.payload.claimAmount = v; state.payload.claimed = v>0 ? false : undefined; state.step='category'; guidedFlow.set(uid, state); }
                const bubble = await buildCategoryPrompt(base, isDbEnabled(), userId||uid);
                await lineReply(replyToken, [{ type:'flex', altText:'選擇分類', contents:bubble }]);
                continue;
              }
              if(dataObj.step==='cat' && dataObj.id){
                state.payload.categoryId = String(dataObj.id);
                state.step = 'note';
                guidedFlow.set(uid, state);
                const bubble = buildNotePrompt(base);
                await lineReply(replyToken, [{ type:'flex', altText:'填寫備註', contents:bubble }]);
                continue;
              }
              if(dataObj.step==='note' && dataObj.skip==='1'){
                // finalize
                const payload = { date: todayYmd(), ...state.payload, note:'' };
                if(isDbEnabled()){
                  await pgdb.addTransaction(userId, payload);
                }else{
                  try{ fileStore.addTransaction(uid, payload); }catch(_){ }
                }
                guidedFlow.delete(uid);
                const bubble = glassFlexBubble({ baseUrl:base, title: payload.type==='income'?'已記收入':'已記支出', subtitle: payload.date, lines:[ `金額：${payload.currency} ${Number(payload.amount||0).toFixed(2)}` ], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'記帳完成', contents:bubble }]);
                continue;
              }
            }
            // Edit flow (multi-turn)
            if(flow==='edit'){
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              const txId = String(dataObj.tx||'');
              const step = String(dataObj.step||'');
              if(step==='cancel'){
                editState.delete(uid);
                const bubble = glassFlexBubble({ baseUrl:base, title:'編輯完成', subtitle:'已離開編輯模式', lines:[], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'編輯完成', contents:bubble }]);
                continue;
              }
              if(!txId){
                const bubble = glassFlexBubble({ baseUrl:base, title:'找不到交易', subtitle:'請從「最近交易」選擇後再試', lines:[], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'找不到交易', contents:bubble }]);
                continue;
              }
              if(step==='choose_cat'){
                const bubble = await buildEditChooseCategoryPrompt(base, isDbEnabled(), userId||uid, txId);
                await lineReply(replyToken, [{ type:'flex', altText:'選擇分類', contents:bubble }]);
                continue;
              }
              if(step==='choose_cat_do' && dataObj.id){
                const catId = String(dataObj.id);
                try{
                  if(isDbEnabled()){
                    await pgdb.updateTransaction(userId, txId, { categoryId: catId });
                  }else{
                    fileStore.updateTransaction(uid, txId, { categoryId: catId });
                  }
                }catch(_){ }
                const bubble = glassFlexBubble({ baseUrl:base, title:'已更新分類', subtitle:'', lines:[], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'已更新', contents:bubble }]);
                continue;
              }
              if(step==='amount' || step==='date' || step==='note'){
                editState.set(uid, { txId, step: `edit_${step}` });
                const bubble = buildEditPromptBubble(base, step, txId);
                await lineReply(replyToken, [{ type:'flex', altText:'輸入新值', contents:bubble }]);
                continue;
              }
              if(step==='claimed'){
                const val = String(dataObj.val||'');
                const claimed = val==='1';
                try{
                  if(isDbEnabled()){
                    await pgdb.updateTransaction(userId, txId, { claimed });
                  }else{
                    fileStore.updateTransaction(uid, txId, { claimed });
                  }
                }catch(_){ }
                const bubble = glassFlexBubble({ baseUrl:base, title: claimed?'已標記為已請款':'已標記為未請款', subtitle:'', lines:[], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'已更新', contents:bubble }]);
                continue;
              }
            }
            // Menu postback fallthrough
            if(dataObj && dataObj.flow==='menu'){
              const bubble = menuFlexBubble({ baseUrl: getBaseUrl(req)||'' });
              await lineReply(replyToken, [{ type:'flex', altText:'功能選單', contents:bubble }]);
              continue;
            }
            // Transaction actions (delete)
            if(flow==='tx'){
              const action = dataObj.action;
              const id = dataObj.id||'';
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              if(action==='delete'){
                const step = dataObj.step||'confirm';
                if(step==='confirm'){
                  const bubble = glassFlexBubble({
                    baseUrl: base,
                    title:'刪除確認',
                    subtitle:'確定要刪除此筆交易嗎？此動作無法復原',
                    lines:[],
                    buttons:[
                      { style:'primary', color:'#dc2626', action:{ type:'postback', label:'確認刪除', data:`flow=tx&action=delete&id=${encodeURIComponent(id)}&step=do` } },
                      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'取消', data:'flow=tx&action=cancel' } }
                    ],
                    showHero:false,
                    compact:true
                  });
                  await lineReply(replyToken, [{ type:'flex', altText:'刪除確認', contents:bubble }]);
                  continue;
                }
                if(step==='do'){
                  try{
                    if(isDbEnabled()){
                      await pgdb.deleteTransaction(userId, id);
                    }else{
                      fileStore.deleteTransaction(uid, id);
                    }
                  }catch(_){ /* ignore */ }
                  const bubble = glassFlexBubble({ baseUrl:base, title:'已刪除', subtitle:'這筆交易已刪除', lines:[], buttons:[ { style:'secondary', action:{ type:'message', label:'查看最近交易', text:'最近交易' } } ], showHero:false, compact:true });
                  await lineReply(replyToken, [{ type:'flex', altText:'刪除完成', contents:bubble }]);
                  continue;
                }
              }
              if(action==='cancel'){
                const bubble = glassFlexBubble({ baseUrl:base, title:'已取消', subtitle:'已取消動作', lines:[], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'已取消', contents:bubble }]);
                continue;
              }
            }
          }
          if(ev.type==='message' && ev.message?.type==='text'){
            const replyToken = ev.replyToken;
            const lineUidRaw = ev.source?.userId || '';
            let userId = lineUidRaw ? `line:${lineUidRaw}` : null;
            // if link table exists and has mapping, use mapped web user id
            try{
              if(lineUidRaw){
                if(isDbEnabled()){
                  const mapped = await pgdb.getLinkedWebUser(`line:${lineUidRaw}`);
                  if(mapped) userId = mapped;
                } else {
                  const mapped = linkStore.getLinkedWebUser(`line:${lineUidRaw}`);
                  if(mapped) userId = mapped;
                }
              }
            }catch(_){ }
            const text = String(ev.message.text||'').trim();
            const normalized = text.replace(/\s+/g,'');
            // Feature menu trigger
            if(/^功能$|^選單$|^menu$/i.test(text)){
              const buttons = [
                { style:'secondary', color:'#64748b', action:{ type:'postback', label:'文字生成', data:'flow=feature&step=responses_text' } },
                { style:'secondary', color:'#64748b', action:{ type:'postback', label:'圖片理解', data:'flow=feature&step=vision' } },
                { style:'secondary', color:'#64748b', action:{ type:'postback', label:'網頁搜尋', data:'flow=feature&step=web_search' } },
                { style:'secondary', color:'#64748b', action:{ type:'postback', label:'文件搜尋', data:'flow=feature&step=file_search' } },
                { style:'secondary', color:'#64748b', action:{ type:'postback', label:'函式工具', data:'flow=feature&step=function_tools' } },
                { style:'secondary', color:'#64748b', action:{ type:'postback', label:'串流示範', data:'flow=feature&step=stream' } }
              ];
              const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req)||'', title:'AI 功能選單', subtitle:'選擇一項示範', lines:[], buttons, showHero:false, compact:true });
              await lineReply(replyToken, [{ type:'flex', altText:'功能選單', contents:bubble }]);
              continue;
            }
            // Admin: open menu or broadcast input handler
            if(isLineAdmin(lineUidRaw)){
              if(/^管理$|^admin$/i.test(text)){
                const bubble = adminMenuBubble({ baseUrl:getBaseUrl(req)||'' });
                await lineReply(replyToken, [{ type:'flex', altText:'管理選單', contents:bubble }]);
                continue;
              }
              const astate = adminState.get(`line:${lineUidRaw}`);
              if(astate && astate.mode==='broadcast_wait'){
                // send broadcast
                try{
                  // collect targets
                  let targets = [];
                  if(isDbEnabled()){
                    const ids = await pgdb.listAllLineUserIds();
                    targets = Array.isArray(ids)?ids:[];
                  }else{
                    const map = readJson(sharedPath('links.json'), {});
                    targets = Object.keys(map||{});
                  }
                  if(targets.length===0 && ADMIN_LINE_USER_ID){ targets=[ADMIN_LINE_USER_ID]; }
                  const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req), title:'系統公告', subtitle: new Date().toLocaleString('zh-TW'), lines:[ String(text) ], showHero:false, compact:true });
                  let success=0;
                  for(const to of targets){ const ok = await linePush(to, [{ type:'flex', altText:'系統公告', contents:bubble }]); if(ok) success++; }
                  adminState.delete(`line:${lineUidRaw}`);
                  await lineReply(replyToken, [{ type:'text', text:`已發送公告：${success}/${targets.length}` }]);
                }catch(_){ await lineReply(replyToken, [{ type:'text', text:'發送失敗' }]); }
                continue;
              }
            }
            // First: contextual AI handler (fraud/revert etc.)
            {
              const handled = await handleContextualAI(req, replyToken, userId, lineUidRaw, text);
              if(handled){ continue; }
            }
            // If in edit mode waiting for user input
            {
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              const st = editState.get(uid);
              if(st && st.txId && /^edit_/.test(st.step||'')){
                const kind = st.step.replace(/^edit_/, '');
                let patch = {};
                if(kind==='amount'){
                  const n = Number(text.match(/([0-9]+(?:\.[0-9]+)?)/)?.[1]||NaN);
                  if(Number.isFinite(n) && n>0){ patch.amount = n; }
                }else if(kind==='date'){
                  let d = null;
                  if(/今天/.test(text)) d = new Date();
                  else if(/昨天|昨日/.test(text)){ d = new Date(); d.setDate(d.getDate()-1); }
                  else if(/前天/.test(text)){ d = new Date(); d.setDate(d.getDate()-2); }
                  else if(/明天/.test(text)){ d = new Date(); d.setDate(d.getDate()+1); }
                  const iso = text.match(/(20\d{2})[-\/]?(\d{1,2})[-\/]?(\d{1,2})/);
                  if(iso){ const y=Number(iso[1]); const m=String(Number(iso[2])).padStart(2,'0'); const dd=String(Number(iso[3])).padStart(2,'0'); patch.date=`${y}-${m}-${dd}`; }
                  else if(d){ patch.date = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`; }
                }else if(kind==='note'){
                  patch.note = text.slice(0,200);
                }
                if(Object.keys(patch).length>0){
                  try{
                    if(isDbEnabled()) await pgdb.updateTransaction(userId, st.txId, patch);
                    else fileStore.updateTransaction(uid, st.txId, patch);
                  }catch(_){ }
                  editState.delete(uid);
                  const bubble = glassFlexBubble({ baseUrl: getBaseUrl(req)||'', title:'已更新', subtitle:'變更已套用', lines:[], showHero:false, compact:true });
                  await lineReply(replyToken, [{ type:'flex', altText:'已更新', contents:bubble }]);
                  continue;
                }else{
                  const bubble = buildEditPromptBubble(getBaseUrl(req)||'', kind, st.txId);
                  await lineReply(replyToken, [{ type:'flex', altText:'請重新輸入', contents:bubble }]);
                  continue;
                }
              }
            }
            // If user is in guided flow, handle by step
            {
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              const state = guidedFlow.get(uid);
              if(state){
                const base = getBaseUrl(req)||'';
                if(state.step==='amount'){
                  const n = Number(text.match(/([0-9]+(?:\.[0-9]+)?)/)?.[1]||NaN);
                  if(Number.isFinite(n) && n>0){
                    state.payload.amount = n; state.step='claim_ask'; guidedFlow.set(uid, state);
                    const bubble = buildClaimAskPrompt(base);
                    await lineReply(replyToken, [{ type:'flex', altText:'是否新增請款金額', contents:bubble }]);
                    continue;
                  }
                  const bubble = buildAmountPrompt(base);
                  await lineReply(replyToken, [{ type:'flex', altText:'輸入金額', contents:bubble }]);
                  continue;
                }
                if(state.step==='claim_amount'){
                  const n = Number(text.match(/([0-9]+(?:\.[0-9]+)?)/)?.[1]||NaN);
                  if(Number.isFinite(n) && n>=0){
                    state.payload.claimAmount = n; state.payload.claimed = n>0 ? false : undefined; state.step='category'; guidedFlow.set(uid, state);
                    const bubble = await buildCategoryPrompt(base, isDbEnabled(), userId||uid);
                    await lineReply(replyToken, [{ type:'flex', altText:'選擇分類', contents:bubble }]);
                    continue;
                  }
                  const bubble = buildClaimAmountPrompt(base, state.payload.amount||0);
                  await lineReply(replyToken, [{ type:'flex', altText:'輸入請款金額', contents:bubble }]);
                  continue;
                }
                if(state.step==='note'){
                  const note = (text==='-'?'':text);
                  const payload = { date: todayYmd(), ...state.payload, note };
                  if(isDbEnabled()){
                    await pgdb.addTransaction(userId, payload);
                  }else{
                    try{ fileStore.addTransaction(uid, payload); }catch(_){ }
                  }
                  guidedFlow.delete(uid);
                  const bubble = glassFlexBubble({ baseUrl:base, title: payload.type==='income'?'已記收入':'已記支出', subtitle: payload.date, lines:[ `金額：${payload.currency} ${Number(payload.amount||0).toFixed(2)}`, note?`備註：${note}`:undefined ].filter(Boolean), showHero:false, compact:true });
                  await lineReply(replyToken, [{ type:'flex', altText:'記帳完成', contents:bubble }]);
                  continue;
                }
              }
            }
            if(normalized==='綁定' || normalized==='绑定'){
              if(!isDbEnabled() && lineUidRaw){
                const base = getBaseUrl(req) || '';
                const code = linkStore.createLinkCode(`line:${lineUidRaw}`, 900);
                const linkUrl = `${base}/auth/line/start${code?`?link=${encodeURIComponent(code)}`:''}`;
                const bubble = glassFlexBubble({ baseUrl: base, title:'綁定帳號', subtitle:'登入網站即可完成綁定', lines:[ '完成後可直接用 LINE 記帳與查詢統計。' ], buttons:[ { style:'primary', action:{ type:'uri', label:'前往綁定', uri: linkUrl } } ] });
                await lineReply(replyToken, [{ type:'flex', altText:'綁定帳號', contents:bubble }]);
                continue;
              }
              if(isDbEnabled() && lineUidRaw){
                const code = await pgdb.createLinkCode(`line:${lineUidRaw}`, 900);
                const base = getBaseUrl(req) || '';
                const linkUrl = `${base}/auth/line/start?link=${encodeURIComponent(code)}`;
                const bubble = glassFlexBubble({ baseUrl: base, title:'綁定帳號', subtitle:'登入網站即可完成綁定', lines:[ '完成後可直接用 LINE 記帳與查詢統計。' ], buttons:[ { style:'primary', action:{ type:'uri', label:'前往綁定', uri: linkUrl } }, { style:'link', action:{ type:'uri', label:'瞭解更多', uri: base||'https://example.com' } } ] });
                await lineReply(replyToken, [{ type:'flex', altText:'綁定帳號', contents:bubble }]);
                continue;
              } else {
                // DB 未啟用或未取得使用者 ID，回覆指引（Flex + 按鈕）
                const base = getBaseUrl(req) || '';
                const bubble = glassFlexBubble({
                  baseUrl: base,
                  title: '需要系統設定',
                  subtitle: '請先設定資料庫或完成登入綁定',
                  lines: ['請先完成系統設定後再輸入「綁定」。如需協助請告知管理者。'],
                  buttons: [
                    { style:'primary', color:'#0ea5e9', action:{ type:'uri', label:'前往登入', uri: `${base||'https://example.com'}` } },
                    { style:'link', action:{ type:'message', label:'使用說明', text:'使用說明' } }
                  ]
                });
                await lineReply(replyToken, [{ type:'flex', altText:'需要系統設定', contents:bubble }]);
                continue;
              }
            }
            // Main menu
            if(/^(選單|功能|menu)$/i.test(text)){
              const bubble = menuFlexBubble({ baseUrl: getBaseUrl(req)||'' });
              await lineReply(replyToken, [{ type:'flex', altText:'功能選單', contents:bubble }]);
              continue;
            }
            // Quick start guided add
            if(/^(記一筆|快速記帳|新增)$/.test(text)){
              const base = getBaseUrl(req)||'';
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              guidedFlow.set(uid, { step:'amount', payload:{ type:'expense', currency:'TWD', rate:1 } });
              const bubble = buildAmountPrompt(base);
              await lineReply(replyToken, [{ type:'flex', altText:'輸入金額', contents:bubble }]);
              continue;
            }
            // Quick intent: stats → 回覆玻璃風格 Flex
            const stats = await handleStatsQuery(userId, text);
            if(stats){
              const bubble = glassFlexBubble({
                baseUrl: getBaseUrl(req),
                title: '統計摘要',
                subtitle: '本月收入/支出/結餘',
                lines: [stats],
                buttons:[
                  { style:'primary', action:{ type:'message', label:'最近交易', text:'最近交易' } },
                  { style:'link', action:{ type:'message', label:'分類支出', text:'分類支出' } }
                ]
              });
              await lineReply(replyToken, [{ type:'flex', altText: '統計摘要', contents: bubble }]);
              continue;
            }

            // 最近交易（前 10 筆）
            if(/最近(交易|紀錄)/.test(text)){
              try{ console.log('[line] intent: recent_transactions'); }catch(_){ }
              if(isDbEnabled()){
                const txs = await pgdb.getTransactions(userId);
                const cats = await pgdb.getCategories();
                const map = new Map(cats.map(c=>[c.id,c.name]));
                const items = txs.slice(0,10);
                const bubbles = items.map(t=> glassFlexBubble({
                  baseUrl: getBaseUrl(req),
                  title: t.type==='income' ? '收入' : '支出',
                  subtitle: `${t.date} ・ ${map.get(t.categoryId)||t.categoryId}`,
                  lines: [
                    `金額：${t.currency||'TWD'} ${Number(t.amount||0).toFixed(2)}`,
                    (Number(t.claimAmount||0)>0) ? `請款：${t.currency||'TWD'} ${Number(t.claimAmount||0).toFixed(2)}` : undefined,
                    t.claimed ? '狀態：已請款' : (t.type==='expense' ? '狀態：未請款' : undefined),
                    t.note ? `備註：${t.note}` : undefined
                  ].filter(Boolean),
                  buttons:[
                    { style:'secondary', color:'#64748b', action:{ type:'postback', label:'刪除', data:`flow=tx&action=delete&id=${encodeURIComponent(t.id)}` } },
                    { style:'link', action:{ type:'uri', label:'編輯', uri: (getBaseUrl(req)||'').replace(/\/$/,'/') }
                    }
                  ],
                  showHero:false,
                  compact:true
                }));
                const contents = bubbles.length>1 ? { type:'carousel', contents:bubbles } : bubbles[0]||glassFlexBubble({ baseUrl:getBaseUrl(req), title:'最近交易', subtitle:'沒有資料', lines:['目前沒有交易'] });
                await lineReply(replyToken, [{ type:'flex', altText:'最近交易', contents }]);
            }else{
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              const txs = fileStore.getTransactions(uid);
              const cats = fileStore.getCategories(uid);
              const map = new Map(cats.map(c=>[c.id,c.name]));
              const items = txs.slice(0,10);
              const bubbles = items.map(t=> glassFlexBubble({
                baseUrl: getBaseUrl(req),
                title: t.type==='income' ? '收入' : '支出',
                subtitle: `${t.date} ・ ${map.get(t.categoryId)||t.categoryId}`,
                lines: [
                  `金額：${t.currency||'TWD'} ${Number(t.amount||0).toFixed(2)}`,
                  (Number(t.claimAmount||0)>0) ? `請款：${t.currency||'TWD'} ${Number(t.claimAmount||0).toFixed(2)}` : undefined,
                  t.claimed ? '狀態：已請款' : (t.type==='expense' ? '狀態：未請款' : undefined),
                  t.note ? `備註：${t.note}` : undefined
                  ].filter(Boolean),
                  buttons:[
                    { style:'secondary', color:'#64748b', action:{ type:'postback', label:'刪除', data:`flow=tx&action=delete&id=${encodeURIComponent(t.id)}` } },
                    { style:'link', action:{ type:'uri', label:'編輯', uri: (getBaseUrl(req)||'').replace(/\/$/,'/') }
                    }
                  ],
                  showHero:false,
                  compact:true
              }));
              const contents = bubbles.length>1 ? { type:'carousel', contents:bubbles } : bubbles[0]||glassFlexBubble({ baseUrl:getBaseUrl(req), title:'最近交易', subtitle:'沒有資料', lines:['目前沒有交易'] });
              await lineReply(replyToken, [{ type:'flex', altText:'最近交易', contents }]);
              }
              continue;
            }

            // 未請款（前 10 筆）
            if(/未請款/.test(text)){
              if(isDbEnabled()){
                const txs = await pgdb.getTransactions(userId);
                const cats = await pgdb.getCategories();
                const map = new Map(cats.map(c=>[c.id,c.name]));
                const items = txs.filter(t=> t.type==='expense' && t.claimed!==true).slice(0,10);
                const lines = items.length>0 ? items.map(t=> `${t.date}｜${map.get(t.categoryId)||t.categoryId}｜${t.currency||'TWD'} ${Number(t.amount||0).toFixed(2)}`) : ['目前沒有未請款項目'];
                const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req), title:'未請款清單', subtitle:`筆數：${items.length}（最多顯示 10 筆）`, lines, buttons:[ { style:'link', action:{ type:'message', label:'最近交易', text:'最近交易' } } ] });
                await lineReply(replyToken, [{ type:'flex', altText:'未請款清單', contents:bubble }]);
            }else{
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              const txs = fileStore.getTransactions(uid);
              const cats = fileStore.getCategories(uid);
              const map = new Map(cats.map(c=>[c.id,c.name]));
              const items = txs.filter(t=> t.type==='expense' && t.claimed!==true).slice(0,10);
              const lines = items.length>0 ? items.map(t=> `${t.date}｜${map.get(t.categoryId)||t.categoryId}｜${t.currency||'TWD'} ${Number(t.amount||0).toFixed(2)}`) : ['目前沒有未請款項目'];
              const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req), title:'未請款清單', subtitle:`筆數：${items.length}（最多顯示 10 筆）`, lines, buttons:[ { style:'link', action:{ type:'message', label:'最近交易', text:'最近交易' } } ] });
              await lineReply(replyToken, [{ type:'flex', altText:'未請款清單', contents:bubble }]);
              }
              continue;
            }

            // 分類支出（本月 Top 10）
            if(/(分類|類別).*支出/.test(text)){
              if(isDbEnabled()){
                const txs = await pgdb.getTransactions(userId);
                const cats = await pgdb.getCategories();
                const map = new Map(cats.map(c=>[c.id,c.name]));
                const now = new Date();
                const ym = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
                const monthTx = txs.filter(t=> (t.date||'').startsWith(ym) && t.type==='expense');
                const sums = new Map();
                for(const t of monthTx){ sums.set(t.categoryId, (sums.get(t.categoryId)||0) + Number(t.amount||0)); }
                const rows = Array.from(sums.entries()).sort((a,b)=> b[1]-a[1]).slice(0,10);
                const lines = rows.length>0 ? rows.map(([id,val])=> `${map.get(id)||id}：$${Number(val||0).toFixed(2)}`) : ['本月尚無支出'];
                const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req), title:'分類支出（本月）', subtitle: ym, lines });
                await lineReply(replyToken, [{ type:'flex', altText:'分類支出', contents:bubble }]);
            }else{
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              const txs = fileStore.getTransactions(uid);
              const cats = fileStore.getCategories(uid);
              const map = new Map(cats.map(c=>[c.id,c.name]));
              const now = new Date();
              const ym = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
              const monthTx = txs.filter(t=> (t.date||'').startsWith(ym) && t.type==='expense');
              const sums = new Map();
              for(const t of monthTx){ sums.set(t.categoryId, (sums.get(t.categoryId)||0) + Number(t.amount||0)); }
              const rows = Array.from(sums.entries()).sort((a,b)=> b[1]-a[1]).slice(0,10);
              const lines = rows.length>0 ? rows.map(([id,val])=> `${map.get(id)||id}：$${Number(val||0).toFixed(2)}`) : ['本月尚無支出'];
              const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req), title:'分類支出（本月）', subtitle: ym, lines });
              await lineReply(replyToken, [{ type:'flex', altText:'分類支出', contents:bubble }]);
              }
              continue;
            }
            // Multi-line batch add: split by newline/;； then handle each line
            {
              const lines = String(text||'').split(/\r\n|\n|\r|[;；]/).map(s=>String(s).trim()).filter(Boolean);
              if(lines.length>1){
                let success=0, skipped=0; let lastDate='';
                let categoriesCtx = {};
                try{
                  const cats = await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(userId||'anonymous'));
                  categoriesCtx = { categories: cats };
                }catch(_){ categoriesCtx = {}; }
                for(const line of lines){
                  let parsed = await aiStructParse(line, categoriesCtx);
                  parsed = mergeParsedAmountFromText(line, parsed||{});
                  const localParsed = parseNlpQuick(line);
                  if(!parsed){ parsed = localParsed; }
                  const amt = Number(parsed?.amount);
                  if(!Number.isFinite(amt) || amt<=0){ skipped++; continue; }
                  const payload = {
                    date: parsed.date || lastDate || todayYmd(),
                    type: parsed.type || 'expense',
                    categoryId: '',
                    currency: parsed.currency || 'TWD',
                    rate: Number(parsed.rate)||1,
                    amount: amt,
                    claimAmount: Number(parsed.claimAmount)||0,
                    claimed: parsed.claimed===true,
                    note: String(parsed.note||line||'')
                  };
                  if(parsed.categoryName){
                    try{
                      const cats = await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(userId||'anonymous'));
                      const hit = cats.find(c=> String(c.name).toLowerCase()===String(parsed.categoryName).toLowerCase());
                      if(hit) payload.categoryId = hit.id;
                    }catch(_){ }
                  }
                  if(!payload.categoryId){
                    try{ const cats = await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(userId||'anonymous')); payload.categoryId = cats[0]?.id || 'food'; }catch(_){ payload.categoryId='food'; }
                  }
                  try{
                    if(isDbEnabled()) await pgdb.addTransaction(userId, payload);
                    else { const uid2 = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous'); fileStore.addTransaction(uid2, payload); }
                    success++;
                    if(payload.date) lastDate = payload.date;
                    try{ if(payload.note && payload.categoryId){ await trainModelFor(userId||'anonymous', payload.note, payload.categoryId); } }catch(_){ }
                  }catch(_){ skipped++; }
                }
                const summary = `已新增 ${success} 筆，略過 ${skipped} 筆。`;
                const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req), title:'批次新增完成', subtitle: todayYmd(), lines:[summary], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'批次新增完成', contents:bubble }]);
                continue;
              }
            }

            // Try AI struct parse first
            // Try AI operations intent first (update/delete/stats)
            {
              let opCtx = {};
              try{
                const cats = await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(userId||'anonymous'));
                opCtx = { categories: cats };
              }catch(_){ opCtx = {}; }
              const ops = await aiOpsParse(text, opCtx);
              const intent = validateAndNormalizeOps(ops);
              if(intent && intent.op && intent.op!=='add'){
                const uid2 = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
                if(intent.op==='delete'){
                  const hit = await findBestMatchingTransaction(isDbEnabled()?userId:uid2, intent.criteria||{});
                  if(hit){
                    if(isDbEnabled()) await pgdb.deleteTransaction(userId, hit.id);
                    else fileStore.deleteTransaction(uid2, hit.id);
                    const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req), title:'已刪除', subtitle: hit.date, lines:[`金額：${hit.currency} ${Number(hit.amount).toFixed(2)}`, hit.note?`備註：${hit.note}`:undefined].filter(Boolean), showHero:false, compact:true });
                    await lineReply(replyToken, [{ type:'flex', altText:'已刪除', contents:bubble }]);
                    continue;
                  }
                }
                if(intent.op==='update'){
                  const hit = await findBestMatchingTransaction(isDbEnabled()?userId:uid2, intent.criteria||{});
                  if(hit){
                    const patch={};
                    if(Number.isFinite(Number(intent.patch?.amount)) && Number(intent.patch.amount)>0) patch.amount = Number(intent.patch.amount);
                    if(intent.patch?.date) patch.date = intent.patch.date;
                    if(intent.patch?.note) patch.note = intent.patch.note;
                    if(intent.patch?.type) patch.type = intent.patch.type;
                    if(intent.patch?.currency) patch.currency = intent.patch.currency;
                    if(Number.isFinite(Number(intent.patch?.claimAmount)) && Number(intent.patch.claimAmount)>=0) patch.claimAmount = Number(intent.patch.claimAmount);
                    if(typeof intent.patch?.claimed==='boolean') patch.claimed = intent.patch.claimed;
                    if(intent.patch?.categoryName){
                      try{
                        const cats = await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(uid2));
                        const hitCat = cats.find(c=> String(c.name).toLowerCase()===String(intent.patch.categoryName).toLowerCase());
                        if(hitCat) patch.categoryId = hitCat.id;
                      }catch(_){ }
                    }
                    let updated=null;
                    if(isDbEnabled()) updated = await pgdb.updateTransaction(userId, hit.id, patch);
                    else updated = fileStore.updateTransaction(uid2, hit.id, patch);
                    const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req), title:'已更新', subtitle: updated.date, lines:[`金額：${updated.currency} ${Number(updated.amount).toFixed(2)}`, updated.note?`備註：${updated.note}`:undefined].filter(Boolean), showHero:false, compact:true });
                    await lineReply(replyToken, [{ type:'flex', altText:'已更新', contents:bubble }]);
                    continue;
                  }
                }
                if(intent.op==='stats'){
                  const stats = await handleStatsQuery(userId, text);
                  if(stats){ const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req), title:'統計摘要', subtitle:'本月收入/支出/結餘', lines:[stats] }); await lineReply(replyToken, [{ type:'flex', altText:'統計摘要', contents:bubble }]); continue; }
                }
              }
            }

            let categoriesCtx = {};
            try{
              const cats = await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(userId||'anonymous'));
              categoriesCtx = { categories: cats };
            }catch(_){ categoriesCtx = {}; }
            let parsed = await aiStructParse(text, categoriesCtx);
            parsed = mergeParsedAmountFromText(text, parsed||{});
            if(!parsed){ parsed = parseNlpQuick(text); }
            if(parsed && Number.isFinite(Number(parsed.amount))){
              const payload = {
                date: parsed.date || todayYmd(),
                type: parsed.type || 'expense',
                categoryId: '',
                currency: (parsed.currency||'TWD'),
                rate: Number(parsed.rate)||1,
                amount: Number(parsed.amount)||0,
                claimAmount: Number(parsed.claimAmount)||0,
                claimed: parsed.claimed===true,
                note: String(parsed.note||'')
              };
              // try category inference by name
              if(parsed.categoryName){
                try{
                  const cats = await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(userId||'anonymous'));
                  const hit = cats.find(c=> String(c.name).toLowerCase()===String(parsed.categoryName).toLowerCase());
                  if(hit) payload.categoryId = hit.id;
                }catch(_){ }
              }
              // note-driven keyword mapping (e.g., 麵包 → 餐飲)
              if(!payload.categoryId && parsed.note){
                const noteLower = String(parsed.note).toLowerCase();
                try{
                  const cats = await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(userId||'anonymous'));
                  const keywords = [
                    { kw:['麵包','早餐','午餐','晚餐','餐','咖啡','飲料'], id: (cats.find(c=>c.id==='food')?.id) || (cats.find(c=> /餐/.test(c.name))?.id) },
                    { kw:['捷運','公車','計程車','高鐵','火車','車票','加油'], id: (cats.find(c=>c.id==='transport')?.id) || (cats.find(c=> /交/.test(c.name))?.id) },
                    { kw:['衣服','購買','買了','買一個','買個','網購','蝦皮','momo','蝦皮購物'], id: (cats.find(c=>c.id==='shopping')?.id) || (cats.find(c=> /購/.test(c.name))?.id) }
                  ];
                  for(const rule of keywords){
                    if(rule && rule.id && rule.kw.some(k=> noteLower.includes(k))){ payload.categoryId = rule.id; break; }
                  }
                }catch(_){ }
              }
              // fallback to first category if empty
              if(!payload.categoryId){
                try{ const cats = await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(userId||'anonymous')); payload.categoryId = cats[0]?.id || 'food'; }catch(_){ payload.categoryId='food'; }
              }
              if(isDbEnabled()){
                await pgdb.addTransaction(userId, payload);
              } else {
                const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
                try{ fileStore.addTransaction(uid, payload); }catch(_){ }
              }
              // train local model with note → category
              try{ if(payload.note && payload.categoryId){ await trainModelFor(userId||'anonymous', payload.note, payload.categoryId); } }catch(_){ }
              const bubble = glassFlexBubble({
                baseUrl: getBaseUrl(req),
                title: payload.type==='income' ? '已記收入' : '已記支出',
                subtitle: payload.date,
                lines: [
                  `金額：${payload.currency} ${Number(payload.amount||0).toFixed(2)}`,
                  (Number(payload.claimAmount||0)>0) ? `請款：${payload.currency} ${Number(payload.claimAmount||0).toFixed(2)}` : undefined,
                  payload.claimed ? '狀態：已請款' : (payload.type==='expense' ? '狀態：未請款' : undefined),
                  payload.note ? `備註：${payload.note}` : undefined
                ].filter(Boolean)
              });
              await lineReply(replyToken, [{ type:'flex', altText:'記帳完成', contents:bubble }]);
              continue;
            }
            // Fallback: chat with AI
            {
              const ctx = { transactions: await (isDbEnabled()? pgdb.getTransactions(userId) : fileStore.getTransactions(userId||'anonymous')), categories: await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(userId||'anonymous')), settings: await (isDbEnabled()? pgdb.getSettings(userId) : fileStore.getSettings?.(userId||'anonymous')) };
              const replyText = await aiChatText(text, ctx);
              await lineReply(replyToken, [{ type:'text', text: String(replyText||'') }]);
            }
          }
        }catch(err){ try{ console.error('line handle error', err); }catch(_){ } }
      }
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      return res.end(JSON.stringify({ ok: true }));
    }

    // Create a one-time binding code (web side) to link current logged-in web user with a LINE user later
    if (req.method === 'POST' && reqPath === '/api/link/create'){
      if(!REQUIRE_AUTH){ res.writeHead(400, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); }
      const user = getUserFromRequest(req);
      if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); }
      const raw = await parseBody(req);
      const { lineUserId='' } = JSON.parse(raw.toString('utf-8')||'{}');
      if(!isDbEnabled() || !lineUserId){ res.writeHead(400, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); }
      await pgdb.upsertLink(lineUserId, user.id || user.userId || '');
      res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify({ ok:true }));
    }

    res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
    return res.end(JSON.stringify({ ok: false, error: 'not_found', path: reqPath, method: req.method }));
  } catch (err) {
    console.error(err);
    res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
    return res.end(JSON.stringify({ ok: false, error: 'server_error' }));
  }
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[ledger-backend] Listening on http://localhost:${PORT}`);
  // kick off background trainer
  try{ startModelTrainer(); }catch(_){ }
});

function heuristicReply(messages, context){
  try{
    const lastUser = (messages||[]).filter(m=>m&&m.role==='user').slice(-1)[0]?.content || '';
    const txs = Array.isArray(context?.transactions) ? context.transactions : [];
    let income=0, expense=0;
    for(const t of txs){
      const type = t.type||'';
      const amt = Number(t.amount)||0;
      if(type==='income') income+=amt; else if(type==='expense') expense+=amt;
    }
    const balance = income-expense;
    return `這是離線建議回覆（未設定 API 金鑰）。最近共 ${txs.length} 筆，收入 $${income.toFixed(2)}、支出 $${expense.toFixed(2)}、結餘 $${balance.toFixed(2)}。\n你的問題：「${lastUser}」。建議：設定每月預算與分類預算，並使用快速記一筆降低阻力。`;
  }catch{
    return '無法產生建議，請稍後再試或設定 AI 供應商。';
  }
}

function fetchJson(url, opts, timeoutMs=15000){
  return new Promise((resolve, reject)=>{
    const u = new URL(url);
    const req = https.request({
      hostname: u.hostname,
      path: u.pathname + u.search,
      method: opts?.method||'GET',
      headers: opts?.headers||{}
    }, (resp)=>{
      const chunks=[];
      resp.on('data', c=>chunks.push(c));
      resp.on('end', ()=>{
        try{ resolve(JSON.parse(Buffer.concat(chunks).toString('utf-8')||'{}')); }
        catch(err){ reject(err); }
      });
    });
    req.on('error', reject);
    const to = setTimeout(()=>{ try{ req.destroy(new Error('upstream timeout')); }catch(_){ } }, timeoutMs);
    req.on('close', ()=>{ clearTimeout(to); });
    if(opts?.body){ req.write(opts.body); }
    req.end();
  });
}


