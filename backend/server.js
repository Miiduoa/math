import http from 'node:http';
import https from 'node:https';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { isDbEnabled, db as pgdb, upsertNoteEmbedding, upsertTxEmbedding, listNoteEmbeddings, listTxEmbeddings } from './db.js';

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
// Allow anonymous access for AI endpoints even if REQUIRE_AUTH=true
const AI_ALLOW_ANON = String(process.env.AI_ALLOW_ANON||'false').toLowerCase()==='true';
// Public base URL for building absolute links in LINE Flex URI buttons (must be https)
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || process.env.BASE_URL || '').replace(/\/$/, '');

// Runtime toggles (in-memory) for ops without redeploy
const runtimeToggles = {
  requireAuth: null,    // null -> use env; boolean overrides
  aiAllowAnon: null     // null -> use env; boolean overrides
};
function isRequireAuth(){ return runtimeToggles.requireAuth===null ? REQUIRE_AUTH : !!runtimeToggles.requireAuth; }
function isAiAllowAnon(){ return runtimeToggles.aiAllowAnon===null ? AI_ALLOW_ANON : !!runtimeToggles.aiAllowAnon; }
// Local model trainer scheduling
const MODEL_TRAIN_INTERVAL_MS = Number(process.env.MODEL_TRAIN_INTERVAL_MS || 300000); // default 5 min
const MODEL_TRAIN_ON_START = String(process.env.MODEL_TRAIN_ON_START||'true').toLowerCase()==='true';
const MODEL_TRAIN_USE_AI = String(process.env.MODEL_TRAIN_USE_AI||'true').toLowerCase()==='true';
const MODEL_TRAIN_AI_MAX_PER_CYCLE = Number(process.env.MODEL_TRAIN_AI_MAX_PER_CYCLE || 20);
const MODEL_TRAIN_RECENT_DAYS = Number(process.env.MODEL_TRAIN_RECENT_DAYS || 7);
// AI config (embeddings & tools)
const EMBEDDING_MODEL = process.env.EMBEDDING_MODEL || 'text-embedding-3-small';
const AI_TOOLS_ENABLED = String(process.env.AI_TOOLS_ENABLED||'true').toLowerCase()==='true';
// AI fallback chains (comma-separated). If unset, use sensible defaults per provider额度。
const OPENAI_FALLBACK_CHAT = (process.env.OPENAI_FALLBACK_CHAT||'').split(',').map(s=>s.trim()).filter(Boolean);
const OPENAI_FALLBACK_STRUCT = (process.env.OPENAI_FALLBACK_STRUCT||'').split(',').map(s=>s.trim()).filter(Boolean);
const runtimeAiStatus = { lastError:null, lastErrorAt:0, lastModelChat:null, lastModelStruct:null };

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
        const notes = readJson(path.join(oldDir,'notes.json'), null); if(Array.isArray(notes)) writeJson(path.join(newDir,'notes.json'), notes);
        const reminders = readJson(path.join(oldDir,'reminders.json'), null); if(Array.isArray(reminders)) writeJson(path.join(newDir,'reminders.json'), reminders);
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

// Simple vector store for file mode
function vectorsPath(userId){ return path.join(userDirFor(userId), 'vectors.json'); }
function getVectorsFile(userId){ const v = readJson(vectorsPath(userId), null); return v && typeof v==='object' ? v : { notes:{}, txs:{} }; }
function setVectorsFile(userId, data){ return writeJson(vectorsPath(userId), data); }
function setNoteEmbeddingFile(userId, id, emb){ const v=getVectorsFile(userId); v.notes= v.notes||{}; v.notes[id]=emb; setVectorsFile(userId,v); }
function setTxEmbeddingFile(userId, id, emb){ const v=getVectorsFile(userId); v.txs= v.txs||{}; v.txs[id]=emb; setVectorsFile(userId,v); }
function deleteNoteEmbeddingFile(userId, id){ const v=getVectorsFile(userId); if(v.notes){ delete v.notes[id]; } setVectorsFile(userId,v); }
function deleteTxEmbeddingFile(userId, id){ const v=getVectorsFile(userId); if(v.txs){ delete v.txs[id]; } setVectorsFile(userId,v); }
function listNoteEmbeddingsFile(userId){ const v=getVectorsFile(userId); return Object.entries(v.notes||{}).map(([id,embedding])=>({id, embedding})); }
function listTxEmbeddingsFile(userId){ const v=getVectorsFile(userId); return Object.entries(v.txs||{}).map(([id,embedding])=>({id, embedding})); }

// Embedding helpers
function getOpenAIBase(){ const base=(process.env.OPENAI_BASE_URL||'https://api.openai.com/v1').replace(/\/+$/,''); return /\/v\d+(?:$|\/)/.test(base)? base: `${base}/v1`; }
async function createEmbedding(input){
  const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';
  const base = getOpenAIBase();
  if(!OPENAI_API_KEY){
    // Fallback: cheap hashed embedding (deterministic) 384 dims
    const dim = 384; const vec = new Array(dim).fill(0);
    const s = String(input||'');
    for(let i=0;i<s.length;i++){
      const cp = s.codePointAt(i)||0; const j = (cp * 2654435761 % dim) >>> 0; vec[j] += 1;
      if(cp>0xffff) i++; // skip surrogate pair
    }
    const norm = Math.sqrt(vec.reduce((a,b)=>a+b*b,0))||1; return vec.map(x=> x/norm);
  }
  try{
    const r = await fetchJson(`${base}/embeddings`,{
      method:'POST', headers:{ 'Authorization':`Bearer ${OPENAI_API_KEY}`, 'Content-Type':'application/json' },
      body: JSON.stringify({ model: EMBEDDING_MODEL, input: String(input||'') })
    }, 20000);
    const emb = r?.data?.[0]?.embedding; if(Array.isArray(emb)) return emb;
  }catch(_){ /* ignore */ }
  // Fallback hashed embedding if provider lacks embeddings
  const dim = 384; const vec = new Array(dim).fill(0);
  const s = String(input||'');
  for(let i=0;i<s.length;i++){
    const cp = s.codePointAt(i)||0;
    const idx = Math.abs(((cp * 1103515245) + 12345) | 0) % dim;
    vec[idx] += 1;
    if(cp>0xffff) i++; // surrogate pair
  }
  const norm = Math.sqrt(vec.reduce((a,b)=>a+b*b,0))||1; return vec.map(x=> x/norm);
}
function cosine(a,b){ let s=0,na=0,nb=0; const n=Math.min(a.length,b.length); for(let i=0;i<n;i++){ const x=a[i]||0, y=b[i]||0; s+=x*y; na+=x*x; nb+=y*y; } const d=(Math.sqrt(na)||1)*(Math.sqrt(nb)||1); return s/d; }
async function upsertNoteVector(userId, note){
  try{
    const text = `${note.title||''} ${note.content||''} ${(note.tags||[]).join(' ')}`.trim();
    if(!text) return false;
    const emb = await createEmbedding(text);
    if(isDbEnabled()) await upsertNoteEmbedding(userId, note.id, emb); else setNoteEmbeddingFile(userId, note.id, emb);
    return true;
  }catch(_){ return false; }
}
async function upsertTxVector(userId, tx, cats){
  try{
    const catName = (id)=> (cats?.find(c=>c.id===id)?.name) || tx.categoryName || tx.categoryId || '';
    const text = `${tx.type||''} ${tx.amount||0} ${tx.currency||'TWD'} ${catName(tx.categoryId)} ${tx.note||''}`.trim();
    if(!text) return false;
    const emb = await createEmbedding(text);
    if(isDbEnabled()) await upsertTxEmbedding(userId, tx.id, emb); else setTxEmbeddingFile(userId, tx.id, emb);
    return true;
  }catch(_){ return false; }
}
async function retrieveByVector(userId, query, topKNotes=8, topKTx=10){
  const q = String(query||'').trim(); if(!q) return { notes:[], txs:[] };
  const qv = await createEmbedding(q);
  let noteVecs = [], txVecs = [];
  try{ noteVecs = isDbEnabled()? await listNoteEmbeddings(userId) : listNoteEmbeddingsFile(userId); }catch(_){ noteVecs=[]; }
  try{ txVecs = isDbEnabled()? await listTxEmbeddings(userId) : listTxEmbeddingsFile(userId); }catch(_){ txVecs=[]; }
  const topNotes = noteVecs.map(r=>({ id:r.id, sc: cosine(qv, r.embedding||[]) }))
    .filter(x=> Number.isFinite(x.sc)).sort((a,b)=> b.sc-a.sc).slice(0, topKNotes);
  const topTx = txVecs.map(r=>({ id:r.id, sc: cosine(qv, r.embedding||[]) }))
    .filter(x=> Number.isFinite(x.sc)).sort((a,b)=> b.sc-a.sc).slice(0, topKTx);
  // load full records for context
  let notes=[], txs=[];
  try{ notes = await (isDbEnabled()? pgdb.getNotes(userId) : getNotes(userId)); }catch(_){ notes=[]; }
  try{ txs = await (isDbEnabled()? pgdb.getTransactions(userId) : fileStore.getTransactions(userId)); }catch(_){ txs=[]; }
  const byId = (arr)=>{ const m=new Map(arr.map(x=>[x.id,x])); return (id)=> m.get(id); };
  const pickNote = byId(notes), pickTx = byId(txs);
  return {
    notes: topNotes.map(x=> pickNote(x.id)).filter(Boolean),
    txs: topTx.map(x=> pickTx(x.id)).filter(Boolean)
  };
}

// Notes & Reminders (file-based)
function safeId(){ try{ return crypto.randomUUID(); }catch(_){ return String(Date.now())+Math.random().toString(16).slice(2); } }
function notesPath(userId){ return path.join(userDirFor(userId), 'notes.json'); }
function remindersPath(userId){ return path.join(userDirFor(userId), 'reminders.json'); }
function getNotes(userId){ return readJson(notesPath(userId), []); }
function setNotes(userId, arr){ return writeJson(notesPath(userId), arr); }
function getReminders(userId){ return readJson(remindersPath(userId), []); }
function setReminders(userId, arr){ return writeJson(remindersPath(userId), arr); }

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

// AI Tools (function-calling) specifications and handlers
function buildToolsSpec(){
  if(!AI_TOOLS_ENABLED) return [];
  return [
    { type:'function', function:{ name:'get_transactions', description:'查詢交易清單', parameters:{ type:'object', properties:{ since:{type:'string', description:'起始日期 YYYY-MM-DD'}, until:{type:'string', description:'結束日期 YYYY-MM-DD'}, type:{type:'string', enum:['income','expense']}, categoryId:{type:'string'}, top:{type:'number'} }, additionalProperties:false } } },
    { type:'function', function:{ name:'add_transaction', description:'新增一筆交易', parameters:{ type:'object', properties:{ date:{type:'string'}, type:{type:'string', enum:['income','expense']}, categoryId:{type:'string'}, currency:{type:'string'}, rate:{type:'number'}, amount:{type:'number'}, claimAmount:{type:'number'}, claimed:{type:'boolean'}, emotion:{type:'string'}, motivation:{type:'string'}, note:{type:'string'} }, required:['date','type','categoryId','amount'], additionalProperties:false } } },
    { type:'function', function:{ name:'update_transaction', description:'更新一筆交易（用 id 或條件匹配）', parameters:{ type:'object', properties:{ id:{type:'string'}, criteria:{ type:'object', properties:{ date:{type:'string'}, amount:{type:'number'}, noteContains:{type:'string'}, type:{type:'string', enum:['income','expense']}, categoryId:{type:'string'} }, additionalProperties:false }, patch:{ type:'object', properties:{ date:{type:'string'}, type:{type:'string', enum:['income','expense']}, categoryId:{type:'string'}, currency:{type:'string'}, rate:{type:'number'}, amount:{type:'number'}, claimAmount:{type:'number'}, claimed:{type:'boolean'}, emotion:{type:'string'}, motivation:{type:'string'}, note:{type:'string'} }, additionalProperties:false } }, additionalProperties:false } } },
    { type:'function', function:{ name:'delete_transaction', description:'刪除一筆交易（用 id 或條件匹配）', parameters:{ type:'object', properties:{ id:{type:'string'}, criteria:{ type:'object', properties:{ date:{type:'string'}, amount:{type:'number'}, noteContains:{type:'string'}, type:{type:'string', enum:['income','expense']}, categoryId:{type:'string'} }, additionalProperties:false } }, additionalProperties:false } } },
    { type:'function', function:{ name:'mark_claimed', description:'標記請款狀態', parameters:{ type:'object', properties:{ id:{type:'string'}, claimed:{type:'boolean'}, claimAmount:{type:'number'} }, required:['id','claimed'], additionalProperties:false } } },
    { type:'function', function:{ name:'get_notes', description:'查詢記事', parameters:{ type:'object', properties:{ q:{type:'string'}, top:{type:'number'} }, additionalProperties:false } } },
    { type:'function', function:{ name:'add_note', description:'新增記事', parameters:{ type:'object', properties:{ title:{type:'string'}, content:{type:'string'}, tags:{type:'array', items:{type:'string'}}, emoji:{type:'string'}, color:{type:'string'}, pinned:{type:'boolean'} }, required:['content'], additionalProperties:false } } },
    { type:'function', function:{ name:'get_stats', description:'取得統計（收入/支出/結餘、未請款、分類排名）', parameters:{ type:'object', properties:{ month:{type:'string', description:'YYYY-MM（優先）'}, since:{type:'string'}, until:{type:'string'}, mode:{type:'string', enum:['range','month'], description:'預設自動'} }, additionalProperties:false } } },
    { type:'function', function:{ name:'budget_delta', description:'計算本月或指定月份與預算差額（TWD）', parameters:{ type:'object', properties:{ month:{type:'string', description:'YYYY-MM；空則當月'} }, additionalProperties:false } } },
    { type:'function', function:{ name:'category_ranking', description:'各分類排行', parameters:{ type:'object', properties:{ month:{type:'string'}, since:{type:'string'}, until:{type:'string'}, type:{type:'string', enum:['expense','income'], description:'預設 expense'}, top:{type:'number'} }, additionalProperties:false } } },
    { type:'function', function:{ name:'quick_report', description:'輸出快速月報（HTML 與摘要）', parameters:{ type:'object', properties:{ month:{type:'string', description:'YYYY-MM；空則當月'} }, additionalProperties:false } } },
    { type:'function', function:{ name:'list_reminders', description:'查詢提醒事項', parameters:{ type:'object', properties:{ top:{type:'number'}, done:{type:'boolean'} }, additionalProperties:false } } },
    { type:'function', function:{ name:'add_reminder', description:'新增提醒事項', parameters:{ type:'object', properties:{ title:{type:'string'}, dueAt:{type:'string'}, repeat:{type:'string', enum:['none','daily','weekly','monthly']}, weekdays:{type:'array', items:{type:'number'}}, monthDay:{type:'number'}, priority:{type:'string', enum:['low','medium','high']}, tags:{type:'array', items:{type:'string'}}, note:{type:'string'} }, required:['title'], additionalProperties:false } } },
    { type:'function', function:{ name:'update_reminder', description:'更新提醒事項', parameters:{ type:'object', properties:{ id:{type:'string'}, patch:{ type:'object', properties:{ title:{type:'string'}, dueAt:{type:'string'}, repeat:{type:'string', enum:['none','daily','weekly','monthly']}, weekdays:{type:'array', items:{type:'number'}}, monthDay:{type:'number'}, priority:{type:'string', enum:['low','medium','high']}, tags:{type:'array', items:{type:'string'}}, note:{type:'string'}, done:{type:'boolean'} }, additionalProperties:false } }, required:['id','patch'], additionalProperties:false } } },
    { type:'function', function:{ name:'delete_reminder', description:'刪除提醒事項', parameters:{ type:'object', properties:{ id:{type:'string'} }, required:['id'], additionalProperties:false } } }
  ];
}
async function callToolByName(name, args, uid){
  try{
    // helpers
    const toBase = (t)=>{ const r=Number(t.rate)||1; const cur=String(t.currency||'TWD'); const amt=Number(t.amount)||0; return cur==='TWD'? amt : (amt*r); };
    function ym(d){ try{ return String(d||'').slice(0,7); }catch(_){ return ''; } }
    function inRange(t, since, until){ const d=String(t.date||''); if(since && d<since) return false; if(until && d>until) return false; return true; }
    function filterByMonth(rows, month){ if(!month) return rows; return rows.filter(t=> ym(t.date)===month); }
    async function loadTx(){ return await (isDbEnabled()? pgdb.getTransactions(uid) : fileStore.getTransactions(uid)); }
    async function loadCats(){ return await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(uid)); }
    async function loadSettings(){ return await (isDbEnabled()? pgdb.getSettings(uid) : fileStore.getSettings(uid)); }
    async function loadReminders(){ return await (isDbEnabled()? pgdb.getReminders(uid) : getReminders(uid)); }
    function pickBestTx(rows, criteria){
      if(!criteria) return rows[0]||null;
      let candidates = rows;
      if(criteria.type){ candidates = candidates.filter(t=> t.type===criteria.type); }
      if(criteria.categoryId){ candidates = candidates.filter(t=> t.categoryId===criteria.categoryId); }
      if(criteria.date){ candidates = candidates.filter(t=> String(t.date||'')===String(criteria.date)); }
      if(Number.isFinite(Number(criteria.amount))){ const a=Number(criteria.amount); candidates = candidates.filter(t=> Math.abs(Number(t.amount||0)-a) < 1e-6); }
      if(criteria.noteContains){ const q=String(criteria.noteContains).toLowerCase(); candidates = candidates.filter(t=> String(t.note||'').toLowerCase().includes(q)); }
      return candidates[0]||null;
    }

    if(name==='get_transactions'){
      let rows = await (isDbEnabled()? pgdb.getTransactions(uid) : fileStore.getTransactions(uid));
      if(args?.type){ rows = rows.filter(x=> x.type===args.type); }
      if(args?.categoryId){ rows = rows.filter(x=> x.categoryId===args.categoryId); }
      if(args?.since){ rows = rows.filter(x=> String(x.date||'') >= String(args.since)); }
      if(args?.until){ rows = rows.filter(x=> String(x.date||'') <= String(args.until)); }
      rows = rows.slice(0, Math.min(200, Number(args?.top)||50));
      return { ok:true, rows };
    }
    if(name==='add_transaction'){
      const rec = await (isDbEnabled()? pgdb.addTransaction(uid, args) : fileStore.addTransaction(uid, args));
      // embed async
      setTimeout(async ()=>{ try{ const cats = await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(uid)); await upsertTxVector(uid, rec, cats); }catch(_){ } }, 0);
      return { ok:true, transaction: rec };
    }
    if(name==='update_transaction'){
      const rows = await loadTx();
      const id = args?.id || '';
      const target = id ? rows.find(t=> t.id===id) : pickBestTx(rows, args?.criteria||{});
      if(!target) return { ok:false, error:'not_found' };
      const patch = args?.patch||{};
      const rec = await (isDbEnabled()? pgdb.updateTransaction(uid, target.id, patch) : fileStore.updateTransaction(uid, target.id, patch));
      if(!rec) return { ok:false, error:'update_failed' };
      setTimeout(async ()=>{ try{ const cats = await loadCats(); await upsertTxVector(uid, rec, cats); }catch(_){ } }, 0);
      return { ok:true, transaction: rec };
    }
    if(name==='delete_transaction'){
      const rows = await loadTx();
      const id = args?.id || '';
      const target = id ? rows.find(t=> t.id===id) : pickBestTx(rows, args?.criteria||{});
      if(!target) return { ok:false, error:'not_found' };
      if(isDbEnabled()) await pgdb.deleteTransaction(uid, target.id); else fileStore.deleteTransaction(uid, target.id);
      setTimeout(()=>{ try{ deleteTxEmbeddingFile(uid, target.id); }catch(_){ } }, 0);
      return { ok:true };
    }
    if(name==='mark_claimed'){
      const id = String(args?.id||''); if(!id) return { ok:false, error:'id_required' };
      const patch = { claimed: !!args?.claimed }; if(Number.isFinite(Number(args?.claimAmount))) patch.claimAmount = Number(args.claimAmount);
      const rec = await (isDbEnabled()? pgdb.updateTransaction(uid, id, patch) : fileStore.updateTransaction(uid, id, patch));
      return { ok: !!rec, transaction: rec };
    }
    if(name==='get_notes'){
      let rows = await (isDbEnabled()? pgdb.getNotes(uid) : getNotes(uid));
      if(args?.q){ const q=String(args.q).toLowerCase(); rows = rows.filter(n=> (n.title||'').toLowerCase().includes(q) || (n.content||'').toLowerCase().includes(q)); }
      rows = rows.slice(0, Math.min(200, Number(args?.top)||50));
      return { ok:true, rows };
    }
    if(name==='add_note'){
      const payload = { title: String(args.title||'').slice(0,120), content: String(args.content||'').slice(0,4000), tags: Array.isArray(args.tags)? args.tags.slice(0,20).map(x=>String(x).slice(0,24)):[], emoji:String(args.emoji||'').slice(0,4), color:String(args.color||'').slice(0,16), pinned: !!args.pinned, archived:false };
      const rec = await (isDbEnabled()? pgdb.addNote(uid, payload) : (function(){ const rows=getNotes(uid); const rec={ id: safeId(), ...payload, createdAt:new Date().toISOString(), updatedAt:new Date().toISOString() }; rows.unshift(rec); setNotes(uid, rows); return rec; })());
      setTimeout(()=>{ upsertNoteVector(uid, rec); }, 0);
      return { ok:true, note: rec };
    }
    if(name==='list_reminders'){
      let rows = await loadReminders();
      if(typeof args?.done==='boolean'){ rows = rows.filter(r=> !!r.done === !!args.done); }
      rows = rows.slice(0, Math.min(200, Number(args?.top)||50));
      return { ok:true, rows };
    }
    if(name==='add_reminder'){
      const payload = { title:String(args.title||'').trim(), dueAt: String(args.dueAt||''), repeat: ['none','daily','weekly','monthly'].includes(String(args.repeat))?String(args.repeat):'none', weekdays: Array.isArray(args.weekdays)? args.weekdays.map(n=> Number(n)|0).filter(n=> n>=0&&n<=6).slice(0,7):[], monthDay: Number.isFinite(Number(args.monthDay))? Number(args.monthDay):undefined, priority: ['low','medium','high'].includes(String(args.priority))?String(args.priority):'medium', tags: Array.isArray(args.tags)? args.tags.slice(0,20).map(x=> String(x).slice(0,24)) : [], note: String(args.note||'') };
      if(!payload.title) return { ok:false, error:'title_required' };
      const rec = await (isDbEnabled()? pgdb.addReminder(uid, payload) : (function(){ const rows=getReminders(uid); const r={ id:safeId(), ...payload, createdAt:new Date().toISOString(), updatedAt:new Date().toISOString(), done:false }; rows.unshift(r); setReminders(uid, rows); return r; })());
      return { ok:true, reminder: rec };
    }
    if(name==='update_reminder'){
      const id = String(args?.id||''); if(!id) return { ok:false, error:'id_required' };
      const patch = args?.patch||{};
      const rec = await (isDbEnabled()? pgdb.updateReminder(uid, id, patch) : (function(){ const rows=getReminders(uid); const idx=rows.findIndex(r=> r.id===id); if(idx<0) return null; const next={ ...rows[idx], ...patch, updatedAt:new Date().toISOString() }; rows[idx]=next; setReminders(uid, rows); return next; })());
      return { ok: !!rec, reminder: rec };
    }
    if(name==='delete_reminder'){
      const id = String(args?.id||''); if(!id) return { ok:false, error:'id_required' };
      if(isDbEnabled()) await pgdb.deleteReminder(uid, id); else { const rows=getReminders(uid).filter(r=> r.id!==id); setReminders(uid, rows); }
      return { ok:true };
    }
    if(name==='get_stats'){
      const month = String(args?.month||'').slice(0,7);
      const since = String(args?.since||'')||null;
      const until = String(args?.until||'')||null;
      let rows = await loadTx();
      if(month){ rows = filterByMonth(rows, month); }
      if(since||until){ rows = rows.filter(t=> inRange(t, since, until)); }
      const income = rows.filter(t=>t.type==='income').reduce((s,t)=> s+toBase(t),0);
      const expense = rows.filter(t=>t.type==='expense').reduce((s,t)=> s+toBase(t),0);
      const net = income-expense;
      const unclaimed = rows.filter(t=> t.type==='expense' && t.claimed !== true);
      const unclaimedSum = unclaimed.reduce((s,t)=> s+toBase(t),0);
      const cats = await loadCats(); const catName=(id)=> (cats.find(c=>c.id===id)?.name)||id;
      const byCat = new Map();
      for(const t of rows){ if(t.type!=='expense') continue; const v=toBase(t); byCat.set(t.categoryId,(byCat.get(t.categoryId)||0)+v); }
      const ranking = Array.from(byCat.entries()).sort((a,b)=>b[1]-a[1]).map(([id,v])=>({ categoryId:id, categoryName:catName(id), amountTWD:v }));
      return { ok:true, stats:{ incomeTWD:income, expenseTWD:expense, netTWD:net, unclaimedCount:unclaimed.length, unclaimedTWD:unclaimedSum, ranking } };
    }
    if(name==='budget_delta'){
      const month = String(args?.month||'').slice(0,7) || (new Date().toISOString().slice(0,7));
      const s = await loadSettings();
      const budget = Number(s?.monthlyBudgetTWD)||0;
      let rows = filterByMonth(await loadTx(), month).filter(t=> t.type==='expense');
      const spent = rows.reduce((sum,t)=> sum+toBase(t),0);
      const delta = budget - spent;
      return { ok:true, month, budgetTWD:budget, spentTWD:spent, deltaTWD:delta, status: delta>=0 ? 'under' : 'over' };
    }
    if(name==='category_ranking'){
      const month = String(args?.month||'').slice(0,7);
      const since = String(args?.since||'')||null;
      const until = String(args?.until||'')||null;
      const type = ['income','expense'].includes(String(args?.type))? String(args.type) : 'expense';
      const top = Math.min(50, Number(args?.top)||10);
      let rows = await loadTx();
      if(month){ rows = filterByMonth(rows, month); }
      if(since||until){ rows = rows.filter(t=> inRange(t, since, until)); }
      const cats = await loadCats(); const catName=(id)=> (cats.find(c=>c.id===id)?.name)||id;
      const byCat = new Map();
      for(const t of rows){ if(t.type!==type) continue; const v=toBase(t); byCat.set(t.categoryId,(byCat.get(t.categoryId)||0)+v); }
      const ranking = Array.from(byCat.entries()).sort((a,b)=>b[1]-a[1]).slice(0,top).map(([id,v])=>({ categoryId:id, categoryName:catName(id), amountTWD:v }));
      return { ok:true, type, month:month||null, ranking };
    }
    if(name==='quick_report'){
      const month = String(args?.month||'').slice(0,7) || (new Date().toISOString().slice(0,7));
      const cats = await loadCats(); const catName=(id)=> (cats.find(c=>c.id===id)?.name)||id;
      const rows = (await loadTx()).filter(t=> ym(t.date)===month);
      const toB = toBase;
      const income = rows.filter(t=>t.type==='income').reduce((s,t)=> s+toB(t),0);
      const expense = rows.filter(t=>t.type==='expense').reduce((s,t)=> s+toB(t),0);
      const net = income-expense;
      const byCat = new Map();
      for(const t of rows){ if(t.type!=='expense') continue; byCat.set(t.categoryId,(byCat.get(t.categoryId)||0)+toB(t)); }
      const ranking = Array.from(byCat.entries()).sort((a,b)=>b[1]-a[1]).slice(0,5).map(([id,v])=>({ id, name:catName(id), amountTWD:v }));
      const fmt = (n)=> new Intl.NumberFormat(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}).format(n);
      const html = `<!doctype html><html><head><meta charset="utf-8"><title>${month} 月報</title><meta name="viewport" content="width=device-width,initial-scale=1"></head><body style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial;padding:16px;line-height:1.5;color:#0f172a"><h2>${month} 月報</h2><p>收入：$${fmt(income)}｜支出：$${fmt(expense)}｜結餘：$${fmt(net)}</p><h3>支出前五分類</h3><ol>${ranking.map(r=>`<li>${r.name} $${fmt(r.amountTWD)}</li>`).join('')}</ol><h3>最近 10 筆</h3><ul>${rows.slice(0,10).map(t=>`<li>${t.date}｜${catName(t.categoryId)}｜${t.type==='income'?'':'-'}$${fmt(toB(t))}｜${(t.note||'').replace(/[<>&]/g,'')}</li>`).join('')}</ul></body></html>`;
      return { ok:true, month, summary:{ incomeTWD:income, expenseTWD:expense, netTWD:net, topCategories:ranking }, html };
    }
  }catch(err){ return { ok:false, error:String(err?.message||err) }; }
  return { ok:false, error:'unknown_tool' };
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
  const raw = String(signed||'');
  const idx = raw.lastIndexOf('.');
  // 開發模式：若未設定 SESSION_SECRET，接受未簽名值（或取點號前的值）
  if(!SESSION_SECRET){
    if(!raw) return null;
    return idx>0 ? raw.slice(0, idx) : raw;
  }
  if(idx<=0) return null;
  const value = raw.slice(0, idx);
  const sig = raw.slice(idx+1);
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
  // Fallback to configured PUBLIC_BASE_URL (should be full https URL)
  if(PUBLIC_BASE_URL){
    if(/^https?:\/\//.test(PUBLIC_BASE_URL)) return PUBLIC_BASE_URL;
    return `https://${PUBLIC_BASE_URL}`;
  }
  return '';
}

// 在允許匿名的情況下，確保每位未登入使用者有固定的簽名 uid cookie（持久化到瀏覽器），避免資料混用
function ensureAnonCookie(req, res){
  if(REQUIRE_AUTH) return null;
  try{
    const cookies = parseCookies(req);
    const existing = verifySigned(cookies['uid']||'');
    if(existing) return existing; // 已存在有效 uid，回傳供當次請求使用
    const anonId = 'anon:' + crypto.randomBytes(12).toString('hex');
    const isHttps = /^https:\/\//.test(getBaseUrl(req)||'');
    setCookie(res, 'uid', createSigned(anonId), { maxAge: 60*60*24*365, secure: isHttps });
    return anonId;
  }catch(_){ return null; }
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
    // 記帳與查詢
    [
      { label:'記一筆', data:'flow=add&step=start', style:'primary' },
      { label:'最近交易', text:'最近交易', style:'secondary' }
    ],
    [
      { label:'未請款', text:'未請款', style:'secondary' },
      { label:'分類排行', text:'分類排行', style:'secondary' }
    ],
    [
      { label:'統計摘要', text:'統計摘要', style:'secondary' },
      { label:'預算差額', text:'預算差額', style:'secondary' }
    ],
    [ { label:'月報', text:'月報', style:'secondary' }, { label:'月曆', text:'月曆', style:'secondary' } ],
    // AI 與批次
    [
      { label:'AI 助理', text:'AI', style:'secondary' },
      { label:'批次新增', text:'批次新增', style:'secondary' }
    ],
    // 記事與提醒（之後將串連到對應 API/意圖）
    [
      { label:'新增記事', text:'新增記事', style:'secondary' },
      { label:'記事清單', text:'記事清單', style:'secondary' }
    ],
    [
      { label:'新增提醒', text:'新增提醒', style:'secondary' },
      { label:'提醒清單', text:'提醒清單', style:'secondary' }
    ],
    // 網頁
    [
      { label:'開啟網頁版', uri: (baseUrl||'https://example.com').replace(/\/$/,'/')+'/#tab=ledger', style:'link' }
    ]
  ];
  const contents = [];
  contents.push({ type:'text', text:'功能選單', weight:'bold', size:'lg', color:'#0f172a' });
  contents.push({ type:'text', text:'快速記帳、查詢、記事、提醒與 AI 助理', size:'xs', color:'#475569', wrap:true, margin:'xs' });
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

// AI contextual intent support (in-memory, single instance)
const aiPending = new Map(); // actionId -> { userId, kind: 'add_tx'|'delete_tx', payload?, txId? }
const aiLastTxId = new Map(); // userId -> last transaction id added via AI
const adminState = new Map(); // userId -> { mode: 'broadcast_wait' }

// Recently-seen payload fingerprints to avoid duplicate inserts across channels (LINE/Web)
const recentTxSeen = new Map(); // userId -> Map<fpr, ts]

function txFingerprint(payload){
  try{
    const date = String(payload.date||'').trim();
    const type = String(payload.type||'').trim();
    const currency = String(payload.currency||'').trim().toUpperCase();
    const amount = String(Number(payload.amount)||0);
    const note = String(payload.note||'').trim().toLowerCase();
    return `${date}|${type}|${currency}|${amount}|${note}`;
  }catch{ return ''; }
}

function seenAndMarkRecent(userId, payload, ttlMs=120000){
  try{
    const fpr = txFingerprint(payload);
    if(!fpr) return false;
    const now = Date.now();
    const uid = userId || 'anonymous';
    let bucket = recentTxSeen.get(uid);
    if(!bucket){ bucket = new Map(); recentTxSeen.set(uid, bucket); }
    // prune old
    for(const [k, ts] of bucket.entries()){ if((now - (ts||0)) > ttlMs) bucket.delete(k); }
    const prev = bucket.get(fpr)||0;
    if(prev && (now - prev) < ttlMs){ return true; }
    bucket.set(fpr, now);
    return false;
  }catch{ return false; }
}

// Notes guided flow
const noteFlow = new Map(); // userId -> { step:'content'|'title'|'tags'|'emoji'|'color'|'confirm', rec }

function buildNoteContentPrompt(base){
  return glassFlexBubble({
    baseUrl: base,
    title: '新增記事',
    subtitle: '步驟 1/5：內容',
    lines:[ '請輸入記事內容' ],
    buttons:[ { style:'link', action:{ type:'postback', label:'取消', data:'flow=note&step=cancel' } } ],
    showHero:false, compact:true
  });
}

function buildNoteTitlePrompt(base){
  return glassFlexBubble({
    baseUrl: base,
    title: '記事標題',
    subtitle: '步驟 2/5：標題（選填）',
    lines:[ '請輸入記事標題，或按「跳過」直接進入下一步' ],
    buttons:[
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'跳過', data:'flow=note&step=title_skip' } },
      { style:'link', action:{ type:'postback', label:'取消', data:'flow=note&step=cancel' } }
    ],
    showHero:false, compact:true
  });
}

function buildNoteTagsPrompt(base){
  return glassFlexBubble({
    baseUrl: base,
    title: '記事標籤',
    subtitle: '步驟 3/5：標籤（選填）',
    lines:[ '請輸入標籤，以空白分隔，或按「跳過」' ],
    buttons:[
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'跳過', data:'flow=note&step=tags_skip' } },
      { style:'link', action:{ type:'postback', label:'取消', data:'flow=note&step=cancel' } }
    ],
    showHero:false, compact:true
  });
}

function buildNoteEmojiPrompt(base){
  return glassFlexBubble({
    baseUrl: base,
    title: '記事表情',
    subtitle: '步驟 4/5：表情符號（選填）',
    lines:[ '請輸入一個表情符號，或按「跳過」' ],
    buttons:[
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'跳過', data:'flow=note&step=emoji_skip' } },
      { style:'link', action:{ type:'postback', label:'取消', data:'flow=note&step=cancel' } }
    ],
    showHero:false, compact:true
  });
}

function buildNoteColorPrompt(base){
  return glassFlexBubble({
    baseUrl: base,
    title: '記事顏色',
    subtitle: '步驟 5/5：顏色（選填）',
    lines:[ '選擇記事顏色' ],
    buttons:[
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'跳過', data:'flow=note&step=color_skip' } },
      { style:'secondary', color:'#ef4444', action:{ type:'postback', label:'紅色', data:'flow=note&step=color&value=red' } },
      { style:'secondary', color:'#3b82f6', action:{ type:'postback', label:'藍色', data:'flow=note&step=color&value=blue' } },
      { style:'secondary', color:'#10b981', action:{ type:'postback', label:'綠色', data:'flow=note&step=color&value=green' } },
      { style:'secondary', color:'#f59e0b', action:{ type:'postback', label:'黃色', data:'flow=note&step=color&value=yellow' } },
      { style:'secondary', color:'#8b5cf6', action:{ type:'postback', label:'紫色', data:'flow=note&step=color&value=purple' } }
    ],
    showHero:false, compact:true
  });
}

function buildNoteConfirmBubble(base, rec){
  const lines = [];
  if(rec.title) lines.push(`標題：${rec.title}`);
  lines.push(`內容：${rec.content.slice(0,60)}${rec.content.length>60?'...':''}`);
  if(rec.tags && rec.tags.length) lines.push(`標籤：${rec.tags.join(', ')}`);
  if(rec.emoji) lines.push(`表情：${rec.emoji}`);
  if(rec.color) lines.push(`顏色：${rec.color}`);
  
  return glassFlexBubble({
    baseUrl: base,
    title: '確認新增記事',
    subtitle: '請確認內容無誤',
    lines,
    buttons:[
      { style:'primary', action:{ type:'postback', label:'確認新增', data:'flow=note&step=confirm&do=1' } },
      { style:'link', action:{ type:'postback', label:'取消', data:'flow=note&step=cancel' } }
    ],
    showHero:false, compact:true
  });
}

// Reminders guided flow (file-based)
const remFlow = new Map(); // userId -> { step:'title'|'due'|'repeat'|'repeat_weekdays'|'monthly_day'|'priority'|'note'|'confirm', rec }
function buildRemTitlePrompt(base){
  return glassFlexBubble({
    baseUrl: base,
    title: '新增提醒',
    subtitle: '步驟 1/6：內容',
    lines:[ '請輸入提醒內容（例如：繳電話費、回電客戶）' ],
    buttons:[ { style:'link', action:{ type:'postback', label:'取消', data:'flow=rem&step=cancel' } } ],
    showHero:false, compact:true
  });
}
function buildRemDuePrompt(base){
  return glassFlexBubble({
    baseUrl: base,
    title: '設定時間',
    subtitle: '步驟 2/6：期限',
    lines:[ '可點快速選項，或輸入：今天/明天/後天/本週X/下週X/月底 + 時間（可省略）' ],
    buttons:[
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'今晚 20:00', data:'flow=rem&step=due&pick=today20' } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'明早 09:00', data:'flow=rem&step=due&pick=tomorrow09' } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'後天 09:00', data:'flow=rem&step=due&pick=dayafter09' } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'本週五 18:00', data:'flow=rem&step=due&pick=thisfri18' } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'下週一 09:00', data:'flow=rem&step=due&pick=nextmon09' } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'月底 18:00', data:'flow=rem&step=due&pick=monthend18' } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'無期限', data:'flow=rem&step=due&pick=none' } }
    ],
    showHero:false, compact:true
  });
}
function buildRemRepeatPrompt(base){
  return glassFlexBubble({
    baseUrl: base,
    title: '是否重複',
    subtitle: '步驟 3/6：週期',
    lines:[ '選擇提醒重複週期' ],
    buttons:[
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'不重複', data:'flow=rem&step=repeat&value=none' } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'每天', data:'flow=rem&step=repeat&value=daily' } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'每週', data:'flow=rem&step=repeat&value=weekly' } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'每月', data:'flow=rem&step=repeat&value=monthly' } }
    ],
    showHero:false, compact:true
  });
}
function buildRemWeekdaysPrompt(base){
  return glassFlexBubble({
    baseUrl: base,
    title: '每週哪幾天',
    subtitle: '步驟 3b/6：輸入 0-6（日=0，一=1）',
    lines:[ '請輸入如：1,3,5；略過可按「跳過」' ],
    buttons:[ { style:'secondary', color:'#64748b', action:{ type:'postback', label:'跳過', data:'flow=rem&step=weekly_days_skip' } } ],
    showHero:false, compact:true
  });
}
function buildRemMonthDayPrompt(base){
  return glassFlexBubble({
    baseUrl: base,
    title: '每月幾號',
    subtitle: '步驟 3b/6：1-31',
    lines:[ '請輸入數字（例如：1 或 28）' ],
    showHero:false, compact:true
  });
}
function buildRemPriorityPrompt(base){
  return glassFlexBubble({
    baseUrl: base,
    title: '優先等級',
    subtitle: '步驟 4/6：選擇',
    lines:[ '選擇提醒優先等級' ],
    buttons:[
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'低', data:'flow=rem&step=priority&value=low' } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'普通', data:'flow=rem&step=priority&value=medium' } },
      { style:'secondary', color:'#64748b', action:{ type:'postback', label:'高', data:'flow=rem&step=priority&value=high' } }
    ],
    showHero:false, compact:true
  });
}
function buildRemNotePrompt(base){
  return glassFlexBubble({
    baseUrl: base,
    title: '備註（選填）',
    subtitle: '步驟 5/6：輸入或略過',
    lines:[ '可直接輸入備註文字，或按「略過」' ],
    buttons:[ { style:'secondary', color:'#64748b', action:{ type:'postback', label:'略過', data:'flow=rem&step=note_skip' } } ],
    showHero:false, compact:true
  });
}
function buildRemConfirmBubble(base, rec){
  const lines = [
    rec.dueAt ? `時間：${new Date(rec.dueAt).toLocaleString('zh-TW')}` : '時間：無期限',
    rec.repeat ? `週期：${rec.repeat}` : '週期：不重複',
    rec.priority ? `優先：${rec.priority}` : undefined,
    rec.note ? `備註：${rec.note}` : undefined
  ].filter(Boolean);
  return glassFlexBubble({
    baseUrl: base,
    title: `確認新增：${rec.title||''}`,
    subtitle: '步驟 6/6：確認',
    lines,
    buttons:[
      { style:'primary', action:{ type:'postback', label:'確認新增', data:'flow=rem&step=confirm&do=1' } },
      { style:'link', action:{ type:'postback', label:'取消', data:'flow=rem&step=cancel' } }
    ],
    showHero:false, compact:true
  });
}

async function computeNudges(userId){
  try{
    const isDb = isDbEnabled();
    const txs = isDb ? await pgdb.getTransactions(userId) : fileStore.getTransactions(userId||'anonymous');
    const settings = isDb ? await pgdb.getSettings(userId) : (fileStore.getSettings?.(userId||'anonymous')||{ baseCurrency:'TWD', monthlyBudgetTWD:0, savingsGoalTWD:0, nudges:true, appearance:'system', categoryBudgets:{} });
    const now = new Date();
    const ym = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
    const thisMonth = txs.filter(t=> (t.date||'').startsWith(ym));
    const toBase = (t)=> Number(t.amount||0) * (t.type==='income'?1:-1);
    const spend = thisMonth.filter(t=>t.type==='expense').reduce((s,t)=> s + (Number(t.amount)||0), 0);
    const income = thisMonth.filter(t=>t.type==='income').reduce((s,t)=> s + (Number(t.amount)||0), 0);
    const unclaimed = txs.filter(t=> t.type==='expense' && t.claimed!==true);
    // frequent item (heuristic): top note keyword
    const noteWords = {};
    for(const t of thisMonth){ const note = String(t.note||'').trim(); if(!note) continue; const w = note.replace(/\s+/g,'').slice(0,6); if(!w) continue; noteWords[w]=(noteWords[w]||0)+1; }
    const topWord = Object.entries(noteWords).sort((a,b)=> b[1]-a[1])[0]?.[0] || '';
    const out = [];
    // Nudge 1: Budget/overspend
    if(Number(settings.monthlyBudgetTWD||0)>0){
      const pct = (spend / Math.max(1, Number(settings.monthlyBudgetTWD))) * 100;
      if(pct >= 80){ out.push({ key:'budget_guardrail', title:`本月支出已達 ${pct.toFixed(0)}% 預算，想設提醒嗎？`, cta:'設定提醒', kind:'settings' }); }
    }else{
      if(spend>=1000){ out.push({ key:'budget_invite', title:'建立每月預算，讓你更容易守住目標', cta:'設定預算', kind:'settings' }); }
    }
    // Nudge 2: Unclaimed expenses
    if(unclaimed.length>=3){ const sumUn = unclaimed.reduce((s,t)=> s+ (Number(t.amount)||0),0); out.push({ key:'unclaimed', title:`有 ${unclaimed.length} 筆未請款（$${sumUn.toFixed(0)}），要安排處理嗎？`, cta:'查看未請款', kind:'unclaimed' }); }
    // Nudge 3: Implementation intention / habit
    if(topWord){ out.push({ key:'habit', title:`下次遇到「${topWord}」時，先檢查是否必要（小步行動）`, cta:'我知道了', kind:'ack' }); }
    // Nudge 4: Savings goal
    if(!Number(settings.savingsGoalTWD||0) && income>0){ out.push({ key:'savings_goal', title:'設定儲蓄目標，提升達成率（目標梯度效應）', cta:'設定目標', kind:'settings' }); }
    return out.slice(0,3);
  }catch{ return []; }
}

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

function getOpenAIClient(){
  try{
    const apiKey = process.env.OPENAI_API_KEY || '';
    if(!apiKey) return null;
    const baseURL = (process.env.OPENAI_BASE_URL || 'https://api.openai.com/v1').replace(/\/+$/,'');
    const mod = require('openai');
    const OpenAI = mod.default || mod.OpenAI || mod;
    return new OpenAI({ apiKey, baseURL });
  }catch(_){ return null; }
}

async function callWithModelFallback(fn){
  const primary = process.env.OPENAI_RESP_MODEL || 'gpt-5';
  const fallback = process.env.OPENAI_FALLBACK_MODEL || 'gpt-4o-mini';
  try{ return await fn(primary); }catch(_){ }
  try{ return await fn(fallback); }catch(_){ }
  return { ok:false, output:'', error:'provider_error' };
}

async function aiResponsesText(userText){
  const client = getOpenAIClient();
  if(!client){
    // fallback to local /api/ai aggregator
    try{ const j = await fetchJson(`http://127.0.0.1:${PORT}/api/ai`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ messages:[{ role:'user', content:String(userText||'') }], mode:'chat' }) }, 12000); return { ok:true, output:String(j?.reply||'') }; }catch(_){ return { ok:false, output:'', error:'no_api_key' }; }
  }
  const r = await callWithModelFallback(async (model)=>{
    const resp = await client.responses.create({ model, input: String(userText||'') });
    return { ok:true, output: String(resp?.output_text||'') };
  });
  if(!r.ok || !r.output){
    try{ const j = await fetchJson(`http://127.0.0.1:${PORT}/api/ai`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ messages:[{ role:'user', content:String(userText||'') }], mode:'chat' }) }, 12000); return { ok:true, output:String(j?.reply||'') }; }catch(_){ }
  }
  return r;
}

async function aiResponsesVision(text, imageUrl){
  const client = getOpenAIClient();
  if(!client){
    try{ const j = await fetchJson(`http://127.0.0.1:${PORT}/api/ai`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ messages:[{ role:'user', content:String(text||'') }], mode:'chat' }) }, 12000); return { ok:true, output:String(j?.reply||'') }; }catch(_){ return { ok:false, output:'', error:'no_api_key' }; }
  }
  const r = await callWithModelFallback(async (model)=>{
    const resp = await client.responses.create({ model, input:[ { role:'user', content:[ { type:'input_text', text: String(text||'') }, { type:'input_image', image_url: String(imageUrl||'') } ] } ] });
    return { ok:true, output: String(resp?.output_text||'') };
  });
  if(!r.ok || !r.output){
    try{ const j = await fetchJson(`http://127.0.0.1:${PORT}/api/ai`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ messages:[{ role:'user', content:String(text||'') }], mode:'chat' }) }, 12000); return { ok:true, output:String(j?.reply||'') }; }catch(_){ }
  }
  return r;
}

async function aiResponsesWebSearch(userText){
  const client = getOpenAIClient();
  if(!client){
    try{ const j = await fetchJson(`http://127.0.0.1:${PORT}/api/ai`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ messages:[{ role:'user', content:String(userText||'') }], mode:'chat' }) }, 12000); return { ok:true, output:String(j?.reply||'') }; }catch(_){ return { ok:false, output:'', error:'no_api_key' }; }
  }
  const r = await callWithModelFallback(async (model)=>{
    const resp = await client.responses.create({ model, tools:[ { type:'web_search' } ], input: String(userText||'') });
    return { ok:true, output: String(resp?.output_text||'') };
  });
  if(!r.ok || !r.output){
    try{ const j = await fetchJson(`http://127.0.0.1:${PORT}/api/ai`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ messages:[{ role:'user', content:String(userText||'') }], mode:'chat' }) }, 12000); return { ok:true, output:String(j?.reply||'') }; }catch(_){ }
  }
  return r;
}

async function aiResponsesFileSearch(userText){
  const client = getOpenAIClient();
  if(!client){
    try{ const j = await fetchJson(`http://127.0.0.1:${PORT}/api/ai`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ messages:[{ role:'user', content:String(userText||'') }], mode:'chat' }) }, 12000); return { ok:true, output:String(j?.reply||'') }; }catch(_){ return { ok:false, output:'', error:'no_api_key' }; }
  }
  const vectorStoreId = process.env.VECTOR_STORE_ID || '';
  if(!vectorStoreId){ return { ok:false, output:'', error:'no_vector_store' }; }
  const r = await callWithModelFallback(async (model)=>{
    const resp = await client.responses.create({ model, input:String(userText||''), tools:[ { type:'file_search', vector_store_ids:[ vectorStoreId ] } ] });
    return { ok:true, output: String(resp?.output_text||'') };
  });
  if(!r.ok || !r.output){
    try{ const j = await fetchJson(`http://127.0.0.1:${PORT}/api/ai`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ messages:[{ role:'user', content:String(userText||'') }], mode:'chat' }) }, 12000); return { ok:true, output:String(j?.reply||'') }; }catch(_){ }
  }
  return r;
}

async function aiResponsesFunctionTool(userText){
  const client = getOpenAIClient();
  if(!client){
    try{ const j = await fetchJson(`http://127.0.0.1:${PORT}/api/ai`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ messages:[{ role:'user', content:String(userText||'') }], mode:'chat' }) }, 12000); return { ok:true, output:String(j?.reply||'') }; }catch(_){ return { ok:false, output:'', error:'no_api_key' }; }
  }
  const tools = [ { type:'function', name:'get_weather', description:'Get current temperature for a given location.', parameters:{ type:'object', properties:{ location:{ type:'string', description:'City and country e.g. Bogotá, Colombia' } }, required:['location'], additionalProperties:false }, strict:true } ];
  const r = await callWithModelFallback(async (model)=>{
    const resp = await client.responses.create({ model, input:[ { role:'user', content:String(userText||'') } ], tools });
    const out = String((resp?.output?.[0] && resp.output[0].to_json && resp.output[0].to_json()) || resp?.output_text || '');
    return { ok:true, output: out };
  });
  if(!r.ok || !r.output){
    try{ const j = await fetchJson(`http://127.0.0.1:${PORT}/api/ai`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ messages:[{ role:'user', content:String(userText||'') }], mode:'chat' }) }, 12000); return { ok:true, output:String(j?.reply||'') }; }catch(_){ }
  }
  return r;
}

async function aiResponsesMcp(userText){
  const client = getOpenAIClient();
  if(!client){
    try{ const j = await fetchJson(`http://127.0.0.1:${PORT}/api/ai`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ messages:[{ role:'user', content:String(userText||'') }], mode:'chat' }) }, 12000); return { ok:true, output:String(j?.reply||'') }; }catch(_){ return { ok:false, output:'', error:'no_api_key' }; }
  }
  const r = await callWithModelFallback(async (model)=>{
    const resp = await client.responses.create({ model, tools:[ { type:'mcp', server_label:'dmcp', server_description:'A Dungeons and Dragons MCP server to assist with dice rolling.', server_url:'https://dmcp-server.deno.dev/sse', require_approval:'never' } ], input:String(userText||'') });
    return { ok:true, output: String(resp?.output_text||'') };
  });
  if(!r.ok || !r.output){
    try{ const j = await fetchJson(`http://127.0.0.1:${PORT}/api/ai`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ messages:[{ role:'user', content:String(userText||'') }], mode:'chat' }) }, 12000); return { ok:true, output:String(j?.reply||'') }; }catch(_){ }
  }
  return r;
}

async function aiResponsesStreamOnce(userText){
  const client = getOpenAIClient();
  if(!client){
    try{ const j = await fetchJson(`http://127.0.0.1:${PORT}/api/ai`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ messages:[{ role:'user', content:String(userText||'') }], mode:'chat' }) }, 12000); return { ok:true, output:String(j?.reply||'') }; }catch(_){ return { ok:false, output:'', error:'no_api_key' }; }
  }
  const primary = process.env.OPENAI_RESP_MODEL || 'gpt-5';
  const fallback = process.env.OPENAI_FALLBACK_MODEL || 'gpt-4o-mini';
  async function run(model){
    const stream = await client.responses.create({ model, input:[ { role:'user', content:String(userText||'') } ], stream:true });
    let out='';
    for await (const ev of stream){ out += String(ev?.output_text||''); }
    return out;
  }
  try{ const out = await run(primary); return { ok: !!out, output: out||'' }; }catch(_){ }
  try{ const out = await run(fallback); return { ok: !!out, output: out||'' }; }catch(_){ }
  return { ok:false, output:'', error:'provider_error' };
}

async function aiAgentsTriage(userText){
  try{
    const mod = await import('@openai/agents');
    const { Agent, run } = mod;
    const spanishAgent = new Agent({ name:'Spanish agent', instructions:'You only speak Spanish.' });
    const englishAgent = new Agent({ name:'English agent', instructions:'You only speak English' });
    const triageAgent = new Agent({ name:'Triage agent', instructions:'Handoff to the appropriate agent based on the language of the request.', handoffs:[ spanishAgent, englishAgent ] });
    const result = await run(triageAgent, String(userText||''));
    return { ok:true, output: String(result?.finalOutput||'') };
  }catch(err){ return { ok:false, output:'', error:'agent_error' }; }
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
      if(isRequireAuth() && !isAiAllowAnon()){
        const user = getUserFromRequest(req);
        if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false, error:'unauthorized' })); }
      }
      const raw = await parseBody(req);
      const body = JSON.parse(raw.toString('utf-8') || '{}');
      const { messages = [], context = {}, mode = 'chat' } = body || {};

      const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';
      const OPENAI_BASE_URL = process.env.OPENAI_BASE_URL || 'https://api.openai.com/v1';
      const OPENAI_MODEL_CHAT = process.env.OPENAI_MODEL_CHAT || process.env.OPENAI_MODEL || 'gpt-4o-mini';
      const OPENAI_MODEL_STRUCT = process.env.OPENAI_MODEL_STRUCT || process.env.OPENAI_MODEL || 'gpt-4o-mini';

      // Fallback: simple heuristic response if no key configured
      if (!OPENAI_API_KEY) {
        const reply = heuristicReply(messages, context);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok: true, provider: 'heuristic', reply }));
      }

      // Lightweight retrieval to augment context with relevant items (notes/transactions)
      async function buildRagAugment(){
        try{
          const q = String(messages?.[messages.length-1]?.content||'').toLowerCase();
          if(!q) return { topTransactions:[], topNotes:[], categories:[] };
          const user = getUserFromRequest(req);
          let uid = user?.id || '';
          if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
          if(!uid) uid = 'anonymous';
          // load data
          const cats = await (isDbEnabled() ? pgdb.getCategories() : fileStore.getCategories(uid));
          let txs = await (isDbEnabled() ? pgdb.getTransactions(uid) : fileStore.getTransactions(uid));
          let notes = [];
          try{ notes = await (isDbEnabled() ? pgdb.getNotes(uid) : getNotes(uid)); }catch(_){ notes = []; }
          // limit candidates (recent-first)
          txs = Array.isArray(txs) ? txs.slice(0,400) : [];
          notes = Array.isArray(notes) ? notes.slice(0,200) : [];
          // simple scoring
          function scoreText(txt){
            const s = String(txt||'').toLowerCase();
            if(!s || !q) return 0;
            let sc = 0;
            const parts = q.split(/[^\p{L}\p{N}]+/u).filter(Boolean);
            for(const w of parts){ if(!w) continue; if(s.includes(w)) sc += w.length>=2 ? 2 : 1; }
            // bonus for full substring
            if(s.includes(q) && q.length>=4) sc += 3;
            return sc;
          }
          const catName = (id)=> (cats.find(c=>c.id===id)?.name)||id;
          const scoredTx = txs.map(t=>({ t, sc: scoreText(`${t.note||''} ${t.categoryName||catName(t.categoryId)||''} ${t.date||''}`) }))
                              .filter(x=> x.sc>0).sort((a,b)=> b.sc-a.sc).slice(0,15)
                              .map(x=>({ id:x.t.id, date:x.t.date, type:x.t.type, amount:x.t.amount, currency:x.t.currency||'TWD', category: catName(x.t.categoryId), note: x.t.note||'' }));
          const scoredNotes = notes.map(n=>({ n, sc: scoreText(`${n.title||''} ${n.content||''}`) }))
                                  .filter(x=> x.sc>0).sort((a,b)=> b.sc-a.sc).slice(0,12)
                                  .map(x=>({ id:x.n.id, title:x.n.title||'', content: String(x.n.content||'').slice(0,240), tags:x.n.tags||[], updatedAt:x.n.updatedAt||x.n.createdAt }));
          return { topTransactions:scoredTx, topNotes:scoredNotes, categories: cats };
        }catch(_){ return { topTransactions:[], topNotes:[], categories:[] }; }
      }

      // Determine uid for retrieval
      let uid = null; try{ const u = getUserFromRequest(req); uid = u?.id||null; }catch(_){ }
      if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
      if(!uid) uid = 'anonymous';
      const lastText = String(messages?.[messages.length-1]?.content||'');
      let vrag = { notes:[], txs:[] };
      try{ if(lastText) vrag = await retrieveByVector(uid, lastText); }catch(_){ vrag = {notes:[], txs:[]}; }
      const rag = await buildRagAugment();

      // Proxy to OpenAI-compatible API
      const payloadBase = {
        model: (mode==='struct') ? OPENAI_MODEL_STRUCT : OPENAI_MODEL_CHAT,
        messages: (mode==='struct') ? [
          { role: 'system', content: `你是一個記帳解析器，請輸出 JSON，包含: type(income|expense), amount(number), currency(string), date(YYYY-MM-DD；若為 MM/DD 則年份取今年；支援 今天/昨天/前天/明天), categoryName(string), rate(number，可省略), claimAmount(number，可省略), claimed(boolean，可省略), note(string)。金額可含中文數字。請優先從提供的分類清單選擇 categoryName，對不到可輸出新名稱，前端會自動建立。${Array.isArray(context?.categories)?'分類清單：'+context.categories.map(c=>c.name).join(', ').slice(0,800):''} 嚴禁將日期/時間/序數視為金額（例如：十月、十點、10:30、10/31、第三次）。若不確定金額，省略 amount 欄位，切勿臆測。只輸出 JSON，不要其他文字。` },
          { role: 'user', content: messages?.[0]?.content || '' }
        ] : [
          { role: 'system', content: 'You are a helpful finance and budgeting assistant for a personal ledger web app. Answer in Traditional Chinese.' },
          { role: 'system', content: `Context JSON (may be partial): ${JSON.stringify(context).slice(0, 4000)}` },
          { role: 'system', content: `Retrieved context (vector): ${JSON.stringify({ topTransactions:vrag.txs, topNotes:vrag.notes }).slice(0, 4000)}` },
          { role: 'system', content: `Retrieved context (lexical): ${JSON.stringify(rag).slice(0, 3000)}` },
          ...messages
        ],
        temperature: (mode==='struct') ? 0 : 0.4,
        max_tokens: (mode==='struct') ? 600 : 1000
      };
      if(mode!=='struct'){
        const tools = buildToolsSpec(); if(tools && tools.length>0){ payloadBase.tools = tools; payloadBase.tool_choice = 'auto'; }
      }
      const base = (OPENAI_BASE_URL || '').replace(/\/+$/,'');
      const apiBase = /\/v\d+(?:$|\/)/.test(base) ? base : `${base}/v1`;
      const endpoint = `${apiBase}/chat/completions`;
      function fallbackList(){
        if(mode==='struct'){
          const prim = (process.env.OPENAI_MODEL_STRUCT||process.env.OPENAI_MODEL||'gpt-5-nano');
          const defaults = [prim, 'gpt-4o-mini', 'gpt-5-mini', 'gpt-4.1-nano', 'gpt-3.5-turbo'];
          const l = OPENAI_FALLBACK_STRUCT.length? OPENAI_FALLBACK_STRUCT : defaults;
          // ensure unique
          return Array.from(new Set(l.filter(Boolean)));
        }else{
          const prim = (process.env.OPENAI_MODEL_CHAT||process.env.OPENAI_MODEL||'gpt-4o-mini');
          const defaults = [prim, 'gpt-4o-mini', 'gpt-5-mini', 'gpt-4.1-mini', 'gpt-5-nano', 'gpt-3.5-turbo'];
          const l = OPENAI_FALLBACK_CHAT.length? OPENAI_FALLBACK_CHAT : defaults;
          return Array.from(new Set(l.filter(Boolean)));
        }
      }
      async function tryModelsWithTools(){
        const models = fallbackList();
        let lastErr = null;
        for(const mdl of models){
          const payload = { ...payloadBase, model: mdl };
          try{
            let data = await fetchJson(endpoint, {
              method: 'POST',
              headers: {
                'Authorization': `Bearer ${OPENAI_API_KEY}`,
                'Content-Type': 'application/json'
              },
              body: JSON.stringify(payload)
            }, 20000);
            let message = data?.choices?.[0]?.message || {};
            if(AI_TOOLS_ENABLED && mode!=='struct' && message?.tool_calls && Array.isArray(message.tool_calls) && message.tool_calls.length>0){
              const toolCall = message.tool_calls[0];
              const name = toolCall?.function?.name||'';
              let args = {};
              try{ args = JSON.parse(toolCall?.function?.arguments||'{}'); }catch(_){ args={}; }
              const result = await callToolByName(name, args, uid);
              const follow = {
                model: mdl,
                messages: [ ...payload.messages, { role:'assistant', tool_calls: [toolCall] }, { role:'tool', tool_call_id: toolCall.id, name, content: JSON.stringify(result) } ],
                temperature: payload.temperature
              };
              data = await fetchJson(endpoint, { method:'POST', headers:{ 'Authorization':`Bearer ${OPENAI_API_KEY}`, 'Content-Type':'application/json' }, body: JSON.stringify(follow) }, 20000);
              message = data?.choices?.[0]?.message || message;
            }
            if(mode==='struct') runtimeAiStatus.lastModelStruct = mdl; else runtimeAiStatus.lastModelChat = mdl;
            return message?.content || '';
          }catch(err){ lastErr = err; continue; }
        }
        runtimeAiStatus.lastError = String(lastErr?.message||lastErr||'unknown');
        runtimeAiStatus.lastErrorAt = Date.now();
        throw lastErr || new Error('all_models_failed');
      }
      try{
        const reply = await tryModelsWithTools();
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
        // Graceful fallback：struct 模式回傳本地解析；chat 模式回傳離線建議
        try{
          if(mode==='struct'){
            const rawText = String(messages?.[0]?.content||'');
            const local = parseNlpQuick(rawText);
            const parsed = validateAndNormalizeStruct({
              type: local.type||'expense',
              amount: Number.isFinite(Number(local.amount)) ? Number(local.amount) : undefined,
              currency: local.currency || 'TWD',
              date: local.date || undefined,
              claimAmount: Number.isFinite(Number(local.claimAmount)) ? Number(local.claimAmount) : undefined,
              claimed: typeof local.claimed==='boolean' ? local.claimed : undefined,
              note: rawText
            });
            res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
            return res.end(JSON.stringify({ ok:true, provider:'fallback', parsed, error:'ai_provider', detail:String(err?.message||err) }));
          }
          const fb = heuristicReply(messages, context) || '（AI 暫時不可用，已回覆離線建議）';
          res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
          return res.end(JSON.stringify({ ok:true, provider:'fallback', reply: fb, error:'ai_provider', detail:String(err?.message||err) }));
        }catch(_){
          res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' });
          return res.end(JSON.stringify({ ok:false, error:'ai_provider', detail: String(err?.message||err) }));
        }
      }
    }

    // AI streaming (SSE)
    if (req.method === 'POST' && reqPath === '/api/ai/stream') {
      if(isRequireAuth() && !isAiAllowAnon()){
        const user = getUserFromRequest(req);
        if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false, error:'unauthorized' })); }
      }
      const raw = await parseBody(req);
      const body = JSON.parse(raw.toString('utf-8') || '{}');
      const { messages = [], context = {} } = body || {};

      const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';
      const OPENAI_BASE_URL = process.env.OPENAI_BASE_URL || 'https://api.openai.com/v1';
      const OPENAI_MODEL_CHAT = process.env.OPENAI_MODEL_CHAT || process.env.OPENAI_MODEL || 'gpt-4o-mini';

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
        // Build lightweight retrieval for streaming as well
        async function buildRagForStream(){
          try{
            const q = String(messages?.[messages.length-1]?.content||'').toLowerCase();
            if(!q) return { topTransactions:[], topNotes:[], categories:[] };
            const user = getUserFromRequest(req);
            let uid = user?.id || '';
            if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
            if(!uid) uid = 'anonymous';
            const cats = await (isDbEnabled() ? pgdb.getCategories() : fileStore.getCategories(uid));
            let txs = await (isDbEnabled() ? pgdb.getTransactions(uid) : fileStore.getTransactions(uid));
            let notes = [];
            try{ notes = await (isDbEnabled() ? pgdb.getNotes(uid) : getNotes(uid)); }catch(_){ notes = []; }
            txs = Array.isArray(txs) ? txs.slice(0,400) : [];
            notes = Array.isArray(notes) ? notes.slice(0,200) : [];
            function scoreText(txt){
              const s = String(txt||'').toLowerCase(); if(!s||!q) return 0; let sc=0; const parts=q.split(/[^\p{L}\p{N}]+/u).filter(Boolean); for(const w of parts){ if(!w) continue; if(s.includes(w)) sc += w.length>=2 ? 2 : 1; } if(s.includes(q) && q.length>=4) sc+=3; return sc;
            }
            const catName = (id)=> (cats.find(c=>c.id===id)?.name)||id;
            const scoredTx = txs.map(t=>({ t, sc: scoreText(`${t.note||''} ${t.categoryName||catName(t.categoryId)||''} ${t.date||''}`) }))
                                .filter(x=> x.sc>0).sort((a,b)=> b.sc-a.sc).slice(0,15)
                                .map(x=>({ id:x.t.id, date:x.t.date, type:x.t.type, amount:x.t.amount, currency:x.t.currency||'TWD', category: catName(x.t.categoryId), note: x.t.note||'' }));
            const scoredNotes = notes.map(n=>({ n, sc: scoreText(`${n.title||''} ${n.content||''}`) }))
                                    .filter(x=> x.sc>0).sort((a,b)=> b.sc-a.sc).slice(0,12)
                                    .map(x=>({ id:x.n.id, title:x.n.title||'', content: String(x.n.content||'').slice(0,240), tags:x.n.tags||[], updatedAt:x.n.updatedAt||x.n.createdAt }));
            // also include vector retrieval
            let vec = { notes:[], txs:[] };
            try{ vec = await retrieveByVector(uid, q); }catch(_){ vec={notes:[], txs:[]}; }
            return { topTransactions:scoredTx, topNotes:scoredNotes, categories: cats, vectorTop: { topTransactions: vec.txs, topNotes: vec.notes } };
          }catch(_){ return { topTransactions:[], topNotes:[], categories:[] }; }
        }
        const ragStream = await buildRagForStream();
        const tools = buildToolsSpec();
        const makePayload = (model)=>({
          model,
          messages: [
            { role: 'system', content: 'You are a helpful finance and budgeting assistant for a personal ledger web app. Answer in Traditional Chinese.' },
            { role: 'system', content: `Context JSON (may be partial): ${JSON.stringify(context).slice(0, 4000)}` },
            { role: 'system', content: `Retrieved context (top matches): ${JSON.stringify(ragStream).slice(0, 4000)}` },
            ...messages
          ],
          temperature: 0.4,
          stream: true,
          ...(tools && tools.length>0 ? { tools, tool_choice:'auto' } : {})
        });
        const models = (OPENAI_FALLBACK_CHAT.length? OPENAI_FALLBACK_CHAT : [process.env.OPENAI_MODEL_CHAT||process.env.OPENAI_MODEL||'gpt-4o-mini','gpt-4o-mini','gpt-5-mini']).filter(Boolean);
        let started = false;
        for(const mdl of models){
          try{
            const payload = makePayload(mdl);
            const reqUp = https.request({
              hostname: endpoint.hostname,
              path: endpoint.pathname + endpoint.search,
              method: 'POST',
              headers: {
                'Authorization': `Bearer ${OPENAI_API_KEY}`,
                'Content-Type': 'application/json'
              }
            }, (resp)=>{
              if(resp.statusCode && resp.statusCode>=400){
                try{ sse({ error:'upstream_status_'+resp.statusCode }); }catch(_){ }
                return;
              }
              started = true;
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
            reqUp.on('error', ()=>{ /* try next */ });
            reqUp.write(JSON.stringify(payload));
            reqUp.end();
            if(started) break;
          }catch(_){ /* try next */ }
        }
        if(!started){ try{ sse({ error:'upstream_error_all' }); }catch(_){ } }
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
      const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req), title:'系統公告', subtitle: new Date().toLocaleString('zh-TW'), lines:[ String(message) ], buttons:[ { style:'link', action:{ type:'uri', label:'開啟網頁版', uri: (getBaseUrl(req)||'https://example.com').replace(/\/$/,'/')+'/#tab=ledger' } } ], showHero:false, compact:true });
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

    // Admin: view effective config/toggles
    if (req.method === 'GET' && reqPath === '/api/admin/config'){
      const ADMIN_KEY = process.env.ADMIN_KEY || '';
      const isAdminByKey = ADMIN_KEY && (String(req.headers['x-admin-key']||'')===ADMIN_KEY);
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
      if(!(isAdminByKey || isAdminByLine)){
        res.writeHead(403, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:false, error:'forbidden' }));
      }
      const hasOpenAIKey = !!(process.env.OPENAI_API_KEY||'').trim();
      const out = {
        ok:true,
        env:{ requireAuth: REQUIRE_AUTH, aiAllowAnon: AI_ALLOW_ANON, hasOpenAIKey, openaiModel: process.env.OPENAI_MODEL||'gpt-4o-mini', openaiBaseUrl: process.env.OPENAI_BASE_URL||'https://api.openai.com/v1' },
        runtime:{ requireAuth: runtimeToggles.requireAuth, aiAllowAnon: runtimeToggles.aiAllowAnon },
        effective:{ requireAuth: isRequireAuth(), aiAllowAnon: isAiAllowAnon() },
        aiStatus:{ lastError: runtimeAiStatus.lastError||null, lastErrorAt: runtimeAiStatus.lastErrorAt||0, lastModelChat: runtimeAiStatus.lastModelChat||null, lastModelStruct: runtimeAiStatus.lastModelStruct||null, fallbackChat: OPENAI_FALLBACK_CHAT, fallbackStruct: OPENAI_FALLBACK_STRUCT }
      };
      res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify(out));
    }

    // LINE diagnostics: verify env/config and webhook reachability
    if (req.method === 'GET' && reqPath === '/api/line/diagnostics'){
      const base = getBaseUrl(req) || '';
      const out = {
        ok: true,
        webhookEndpoint: `${base}/line/webhook`,
        hasChannelSecret: !!(LINE_CHANNEL_SECRET||'').trim(),
        hasAccessToken: !!(LINE_CHANNEL_ACCESS_TOKEN||'').trim(),
        publicBaseUrl: base || PUBLIC_BASE_URL || '',
        suggestions: []
      };
      if(!out.hasAccessToken){ out.suggestions.push('設定 LINE_CHANNEL_ACCESS_TOKEN'); }
      if(!out.hasChannelSecret){ out.suggestions.push('設定 LINE_CHANNEL_SECRET（否則簽章驗證將略過）'); }
      if(!/^https:\/\//.test(base||PUBLIC_BASE_URL||'')){
        out.suggestions.push('設定 PUBLIC_BASE_URL 為 https:// 供 Flex 按鈕使用');
      }
      res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify(out));
    }

    // LINE ping: push a test Flex message to admin to verify buttons
    if (req.method === 'POST' && reqPath === '/api/line/ping'){
      const ADMIN_KEY = process.env.ADMIN_KEY || '';
      const hasKey = ADMIN_KEY && String(req.headers['x-admin-key']||'')===ADMIN_KEY;
      if(!hasKey){ res.writeHead(403, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false, error:'forbidden' })); }
      if(!LINE_CHANNEL_ACCESS_TOKEN){ res.writeHead(400, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false, error:'missing_access_token' })); }
      const to = ADMIN_LINE_USER_ID;
      const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req)||PUBLIC_BASE_URL||'', title:'Ping 測試', subtitle:new Date().toLocaleString('zh-TW'), lines:['這是一則測試訊息','請點選下方按鈕驗證動作'], buttons:[ { style:'primary', action:{ type:'postback', label:'功能選單', data:'flow=menu' } }, { style:'link', action:{ type:'uri', label:'開啟網頁版', uri:(getBaseUrl(req)||PUBLIC_BASE_URL||'').replace(/\/$/,'/')+'/#tab=ledger' } } ], showHero:false, compact:true });
      const ok = await linePush(to, [{ type:'flex', altText:'Ping 測試', contents:bubble }]);
      res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify({ ok }));
    }

    // Admin: update toggles (in-memory) and optional OpenAI config
    if (req.method === 'POST' && reqPath === '/api/admin/toggles'){
      const ADMIN_KEY = process.env.ADMIN_KEY || '';
      const isAdminByKey = ADMIN_KEY && (String(req.headers['x-admin-key']||'')===ADMIN_KEY);
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
      if(!(isAdminByKey || isAdminByLine)){
        res.writeHead(403, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:false, error:'forbidden' }));
      }
      const raw = await parseBody(req);
      let body={};
      try{ body = JSON.parse(raw.toString('utf-8')||'{}'); }catch(_){ body={}; }
      if(typeof body.requireAuth==='boolean') runtimeToggles.requireAuth = !!body.requireAuth;
      if(typeof body.aiAllowAnon==='boolean') runtimeToggles.aiAllowAnon = !!body.aiAllowAnon;
      if(Object.prototype.hasOwnProperty.call(body, 'openaiApiKey')){
        const v = body.openaiApiKey;
        process.env.OPENAI_API_KEY = (v==null) ? '' : String(v);
      }
      if(Object.prototype.hasOwnProperty.call(body, 'openaiBaseUrl')){
        const v = body.openaiBaseUrl;
        process.env.OPENAI_BASE_URL = (v==null) ? '' : String(v);
      }
      if(Object.prototype.hasOwnProperty.call(body, 'openaiModel')){
        const v = body.openaiModel;
        process.env.OPENAI_MODEL = (v==null) ? '' : String(v);
      }
      if(Object.prototype.hasOwnProperty.call(body, 'openaiModelChat')){
        const v = body.openaiModelChat;
        process.env.OPENAI_MODEL_CHAT = (v==null) ? '' : String(v);
      }
      if(Object.prototype.hasOwnProperty.call(body, 'openaiModelStruct')){
        const v = body.openaiModelStruct;
        process.env.OPENAI_MODEL_STRUCT = (v==null) ? '' : String(v);
      }
      const hasOpenAIKey = !!(process.env.OPENAI_API_KEY||'').trim();
      const out = {
        ok:true,
        runtime:{ requireAuth: runtimeToggles.requireAuth, aiAllowAnon: runtimeToggles.aiAllowAnon },
        effective:{ requireAuth: isRequireAuth(), aiAllowAnon: isAiAllowAnon() },
        hasOpenAIKey,
        openaiModel: process.env.OPENAI_MODEL||'gpt-4o-mini',
        openaiModelChat: process.env.OPENAI_MODEL_CHAT||process.env.OPENAI_MODEL||'gpt-4o-mini',
        openaiModelStruct: process.env.OPENAI_MODEL_STRUCT||process.env.OPENAI_MODEL||'gpt-4o-mini',
        openaiBaseUrl: process.env.OPENAI_BASE_URL||'https://api.openai.com/v1'
      };
      res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify(out));
    }

    // Serve static frontend (index.html, styles.css, app.js, db.js, manifest, sw) from project root
    if (req.method === 'GET' && (reqPath === '/' || reqPath === '/index.html' || reqPath === '/styles.css' || reqPath === '/app.js' || reqPath === '/db.js' || reqPath === '/flex-glass.svg' || reqPath === '/manifest.webmanifest' || reqPath === '/sw.js')) {
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
          : ext === '.webmanifest' ? 'application/manifest+json; charset=utf-8'
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
      let uid = user?.id || '';
      if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
      if(!uid) uid = 'anonymous';
      const commonHeaders = { 'Content-Type':'application/json; charset=utf-8', 'Cache-Control':'no-store', 'X-UID': uid, 'X-User-Mode': isDbEnabled()?'db':'file' };
      if(isDbEnabled()){
        const rows = await pgdb.getTransactions(uid);
        res.writeHead(200, commonHeaders);
        return res.end(JSON.stringify(rows));
      } else {
        const rows = fileStore.getTransactions(uid);
        res.writeHead(200, commonHeaders);
        return res.end(JSON.stringify(rows));
      }
    }
    if (req.method === 'POST' && reqPath === '/api/transactions'){
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false, error:'unauthorized' })); } }
      const raw = await parseBody(req);
      const payload = JSON.parse(raw.toString('utf-8') || '{}');
      const user = getUserFromRequest(req);
      let uid = user?.id || '';
      if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
      if(!uid) uid = 'anonymous';
      if(isDbEnabled()){
        const rec = await pgdb.addTransaction(uid, payload);
        setTimeout(async ()=>{ try{ const cats = await pgdb.getCategories(); await upsertTxVector(uid, rec, cats); }catch(_){ } }, 0);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      } else {
        const rec = fileStore.addTransaction(uid, payload);
        setTimeout(async ()=>{ try{ const cats = fileStore.getCategories(uid); await upsertTxVector(uid, rec, cats); }catch(_){ } }, 0);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      }
    }
    if (req.method === 'GET' && reqPath.startsWith('/api/transactions/')){
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const id = decodeURIComponent(reqPath.split('/').pop()||'');
      const user = getUserFromRequest(req);
      let uid = user?.id || '';
      if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
      if(!uid) uid = 'anonymous';
      const commonHeaders = { 'Content-Type':'application/json; charset=utf-8', 'Cache-Control':'no-store', 'X-UID': uid, 'X-User-Mode': isDbEnabled()?'db':'file' };
      if(isDbEnabled()){
        const rec = await pgdb.getTransactionById(uid, id);
        res.writeHead(200, commonHeaders);
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      } else {
        const rec = fileStore.getTransactionById(uid, id);
        res.writeHead(200, commonHeaders);
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      }
    }
    if (req.method === 'PUT' && reqPath.startsWith('/api/transactions/')){
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const id = decodeURIComponent(reqPath.split('/').pop()||'');
      const raw = await parseBody(req);
      const patch = JSON.parse(raw.toString('utf-8') || '{}');
      const user = getUserFromRequest(req);
      let uid = user?.id || '';
      if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
      if(!uid) uid = 'anonymous';
      if(isDbEnabled()){
        const rec = await pgdb.updateTransaction(uid, id, patch);
        if(!rec){ res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); }
        setTimeout(async ()=>{ try{ const cats = await pgdb.getCategories(); await upsertTxVector(uid, rec, cats); }catch(_){ } }, 0);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      } else {
        const rec = fileStore.updateTransaction(uid, id, patch);
        if(!rec){ res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); }
        setTimeout(async ()=>{ try{ const cats = fileStore.getCategories(uid); await upsertTxVector(uid, rec, cats); }catch(_){ } }, 0);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      }
    }
    if (req.method === 'DELETE' && reqPath.startsWith('/api/transactions/')){
      if(REQUIRE_AUTH){ const u = getUserFromRequest(req); if(!u){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const id = decodeURIComponent(reqPath.split('/').pop()||'');
      const user = getUserFromRequest(req);
      let uid = user?.id || '';
      if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
      if(!uid) uid = 'anonymous';
      if(isDbEnabled()){
        await pgdb.deleteTransaction(uid, id);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true }));
      } else {
        const ok = fileStore.deleteTransaction(uid, id); setTimeout(()=>{ deleteTxEmbeddingFile(uid, id); }, 0);
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

    // Reports: monthly HTML (for LINE link)
    if (req.method === 'GET' && reqPath === '/reports/monthly'){
      const urlObj = new URL(req.url||'/', `http://${req.headers.host}`);
      const ym = (urlObj.searchParams.get('ym')||'').slice(0,7) || new Date().toISOString().slice(0,7);
      const user = getUserFromRequest(req);
      let uid = user?.id || '';
      if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
      if(!uid) uid = 'anonymous';
      try{
        const cats = await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(uid));
        const catName=(id)=> (cats.find(c=>c.id===id)?.name)||id;
        const rows = (await (isDbEnabled()? pgdb.getTransactions(uid) : fileStore.getTransactions(uid))).filter(t=> String(t.date||'').slice(0,7)===ym);
        const toB = (t)=>{ const r=Number(t.rate)||1; const cur=String(t.currency||'TWD'); const amt=Number(t.amount)||0; return cur==='TWD'? amt : (amt*r); };
        const income = rows.filter(t=>t.type==='income').reduce((s,t)=> s+toB(t),0);
        const expense = rows.filter(t=>t.type==='expense').reduce((s,t)=> s+toB(t),0);
        const net = income-expense;
        const byCat = new Map();
        for(const t of rows){ if(t.type!=='expense') continue; byCat.set(t.categoryId,(byCat.get(t.categoryId)||0)+toB(t)); }
        const ranking = Array.from(byCat.entries()).sort((a,b)=>b[1]-a[1]).slice(0,5).map(([id,v])=>({ id, name:catName(id), amountTWD:v }));
        const fmt = (n)=> new Intl.NumberFormat(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}).format(n);
        const html = `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>${ym} 月報</title><style>body{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial;margin:0;padding:16px;background:#f8fbff;color:#0f172a}h2{margin:0 0 12px}ol,ul{margin:8px 0 0 18px}</style></head><body><h2>${ym} 月報</h2><p>收入：$${fmt(income)}｜支出：$${fmt(expense)}｜結餘：$${fmt(net)}</p><h3>支出前五分類</h3><ol>${ranking.map(r=>`<li>${r.name} $${fmt(r.amountTWD)}</li>`).join('')}</ol><h3>最近 10 筆</h3><ul>${rows.slice(0,10).map(t=>`<li>${t.date}｜${catName(t.categoryId)}｜${t.type==='income'?'':'-'}$${fmt(toB(t))}｜${(t.note||'').replace(/[<>&]/g,'')}</li>`).join('')}</ul></body></html>`;
        res.writeHead(200, { 'Content-Type':'text/html; charset=utf-8' });
        return res.end(html);
      }catch(err){
        res.writeHead(500, { 'Content-Type':'text/plain; charset=utf-8' });
        return res.end('生成失敗');
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
            
            // Notes guided flow (postback)
            if(flow==='note'){
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              let st = noteFlow.get(uid) || { step:'content', rec:{ title:'', content:'', tags:[], emoji:'', color:'', pinned:false, archived:false } };
              const step = String(dataObj.step||'');
              if(step==='cancel'){
                noteFlow.delete(uid);
                const bubble = glassFlexBubble({ baseUrl:base, title:'已取消', subtitle:'已中止記事流程', lines:[], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'已取消', contents:bubble }]);
                continue;
              }
              if(st.step==='content'){
                // waiting for text, but allow user to restart by pressing menu: show content prompt
                const bubble = buildNoteContentPrompt(base);
                await lineReply(replyToken, [{ type:'flex', altText:'新增記事', contents:bubble }]);
                continue;
              }
              if(step==='title_skip'){
                st.step='tags'; noteFlow.set(uid, st);
                const bubble = buildNoteTagsPrompt(base); await lineReply(replyToken, [{ type:'flex', altText:'記事標籤', contents:bubble }]); continue;
              }
              if(step==='tags_skip'){
                st.step='emoji'; noteFlow.set(uid, st);
                const bubble = buildNoteEmojiPrompt(base); await lineReply(replyToken, [{ type:'flex', altText:'記事表情', contents:bubble }]); continue;
              }
              if(step==='emoji_skip'){
                st.step='color'; noteFlow.set(uid, st);
                const bubble = buildNoteColorPrompt(base); await lineReply(replyToken, [{ type:'flex', altText:'記事顏色', contents:bubble }]); continue;
              }
              if(step==='color_skip' || step==='color'){
                if(step==='color'){
                  const value = String(dataObj.value||'');
                  if(['red','blue','green','yellow','purple'].includes(value)) st.rec.color = value;
                }
                st.step='confirm'; noteFlow.set(uid, st);
                const bubble = buildNoteConfirmBubble(base, st.rec); await lineReply(replyToken, [{ type:'flex', altText:'確認新增', contents:bubble }]); continue;
              }
              if(step==='confirm' && dataObj.do==='1'){
                // finalize
                try{
                  let rec;
                  if(isDbEnabled()){
                    rec = await pgdb.addNote(uid, st.rec);
                  } else {
                    const rows = getNotes(uid);
                    rec = { id: (crypto.randomUUID&&crypto.randomUUID())||String(Date.now()), ...st.rec, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
                    rows.unshift(rec); setNotes(uid, rows);
                  }
                }catch(_){ }
                noteFlow.delete(uid);
                const bubble = glassFlexBubble({ baseUrl:base, title:'已新增記事', subtitle:new Date().toLocaleString('zh-TW'), lines:[ st.rec.content.slice(0,80) ], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'已新增記事', contents:bubble }]);
                continue;
              }
            }
            
            // Reminders guided flow (postback)
            if(flow==='rem'){
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              let st = remFlow.get(uid) || { step:'title', rec:{ title:'', dueAt:'', repeat:'none', weekdays:[], monthDay:undefined, priority:'medium', note:'' } };
              const step = String(dataObj.step||'');
              if(step==='cancel'){
                remFlow.delete(uid);
                const bubble = glassFlexBubble({ baseUrl:base, title:'已取消', subtitle:'已中止提醒流程', lines:[], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'已取消', contents:bubble }]);
                continue;
              }
              if(st.step==='title'){
                // waiting for text, but allow user to restart by pressing menu: show title prompt
                const bubble = buildRemTitlePrompt(base);
                await lineReply(replyToken, [{ type:'flex', altText:'新增提醒', contents:bubble }]);
                continue;
              }
              if(step==='due'){
                const pick = String(dataObj.pick||'');
                let dt='';
                if(pick==='today20'){ const d=new Date(); d.setHours(20,0,0,0); dt=d.toISOString(); }
                else if(pick==='tomorrow09'){ const d=new Date(); d.setDate(d.getDate()+1); d.setHours(9,0,0,0); dt=d.toISOString(); }
                else if(pick==='dayafter09'){ const d=new Date(); d.setDate(d.getDate()+2); d.setHours(9,0,0,0); dt=d.toISOString(); }
                else if(pick==='thisfri18'){ const d=new Date(); const cur=d.getDay(); const target=5; const add=(target-cur+7)%7; d.setDate(d.getDate()+(add||0)); d.setHours(18,0,0,0); dt=d.toISOString(); }
                else if(pick==='nextmon09'){ const d=new Date(); const cur=d.getDay(); const add=((1 - cur + 7) % 7) || 7; d.setDate(d.getDate()+add); d.setHours(9,0,0,0); dt=d.toISOString(); }
                else if(pick==='monthend18'){ const d=new Date(); d.setMonth(d.getMonth()+1, 0); d.setHours(18,0,0,0); dt=d.toISOString(); }
                else if(pick==='none'){ dt=''; }
                if(dt!==undefined){ st.rec.dueAt = dt; st.step='repeat'; remFlow.set(uid, st); const bubble = buildRemRepeatPrompt(base); await lineReply(replyToken, [{ type:'flex', altText:'設定週期', contents:bubble }]); continue; }
              }
              if(step==='repeat'){
                const v = String(dataObj.value||'none');
                st.rec.repeat = v; remFlow.set(uid, st);
                if(v==='weekly'){ st.step='repeat_weekdays'; const bubble = buildRemWeekdaysPrompt(base); await lineReply(replyToken, [{ type:'flex', altText:'選擇星期', contents:bubble }]); continue; }
                if(v==='monthly'){ st.step='monthly_day'; const bubble = buildRemMonthDayPrompt(base); await lineReply(replyToken, [{ type:'flex', altText:'每月幾號', contents:bubble }]); continue; }
                st.step='priority'; const bubble = buildRemPriorityPrompt(base); await lineReply(replyToken, [{ type:'flex', altText:'設定優先', contents:bubble }]); continue;
              }
              if(step==='weekly_days_skip'){
                st.rec.weekdays=[]; st.step='priority'; remFlow.set(uid, st);
                const bubble = buildRemPriorityPrompt(base); await lineReply(replyToken, [{ type:'flex', altText:'設定優先', contents:bubble }]); continue;
              }
              if(step==='priority'){
                const v = String(dataObj.value||'medium');
                st.rec.priority = (v==='low'||v==='high') ? v : 'medium';
                st.step='note'; remFlow.set(uid, st);
                const bubble = buildRemNotePrompt(base);
                await lineReply(replyToken, [{ type:'flex', altText:'新增備註', contents:bubble }]);
                continue;
              }
              if(step==='note_skip'){
                st.rec.note=''; st.step='confirm'; remFlow.set(uid, st);
                const bubble = buildRemConfirmBubble(base, st.rec); await lineReply(replyToken, [{ type:'flex', altText:'確認新增', contents:bubble }]); continue;
              }
              if(step==='confirm' && dataObj.do==='1'){
                // finalize
                try{
                  let rec;
                  if(isDbEnabled()){
                    rec = await pgdb.addReminder(uid, st.rec);
                  } else {
                    const rows = getReminders(uid);
                    rec = { id: (crypto.randomUUID&&crypto.randomUUID())||String(Date.now()), ...st.rec, createdAt:new Date().toISOString(), updatedAt:new Date().toISOString() };
                    rows.unshift(rec); setReminders(uid, rows);
                  }
                }catch(_){ }
                remFlow.delete(uid);
                const bubble = glassFlexBubble({ baseUrl:base, title:'已新增提醒', subtitle:new Date().toLocaleString('zh-TW'), lines:[ st.rec.title||'' ], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'已新增提醒', contents:bubble }]);
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
            
            // Notes guided flow (message input)
            {
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              let st = noteFlow.get(uid);
              if(st){
                if(st.step==='content'){
                  st.rec.content = text;
                  st.step='title'; noteFlow.set(uid, st);
                  const bubble = buildNoteTitlePrompt(getBaseUrl(req)||'');
                  await lineReply(replyToken, [{ type:'flex', altText:'記事標題', contents:bubble }]);
                  continue;
                }
                if(st.step==='title'){
                  st.rec.title = text;
                  st.step='tags'; noteFlow.set(uid, st);
                  const bubble = buildNoteTagsPrompt(getBaseUrl(req)||'');
                  await lineReply(replyToken, [{ type:'flex', altText:'記事標籤', contents:bubble }]);
                  continue;
                }
                if(st.step==='tags'){
                  const tags = text.split(/\s+/).filter(t=>t.length>0).slice(0,20);
                  st.rec.tags = tags;
                  st.step='emoji'; noteFlow.set(uid, st);
                  const bubble = buildNoteEmojiPrompt(getBaseUrl(req)||'');
                  await lineReply(replyToken, [{ type:'flex', altText:'記事表情', contents:bubble }]);
                  continue;
                }
                if(st.step==='emoji'){
                  st.rec.emoji = text.slice(0,4);
                  st.step='color'; noteFlow.set(uid, st);
                  const bubble = buildNoteColorPrompt(getBaseUrl(req)||'');
                  await lineReply(replyToken, [{ type:'flex', altText:'記事顏色', contents:bubble }]);
                  continue;
                }
              }
            }
            
            // Reminders guided flow (message input)
            {
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              let st = remFlow.get(uid);
              if(/^(新增提醒|提醒)$/.test(text)){
                st = { step:'title', rec:{ title:'', dueAt:'', repeat:'none', weekdays:[], monthDay:undefined, priority:'medium', note:'' } };
                remFlow.set(uid, st);
                const bubble = buildRemTitlePrompt(getBaseUrl(req)||'');
                await lineReply(replyToken, [{ type:'flex', altText:'新增提醒', contents:bubble }]);
                continue;
              }
              if(st){
                if(st.step==='title'){
                  st.rec.title = text;
                  st.step='due'; remFlow.set(uid, st);
                  const bubble = buildRemDuePrompt(getBaseUrl(req)||'');
                  await lineReply(replyToken, [{ type:'flex', altText:'設定期限', contents:bubble }]);
                  continue;
                }
                if(st.step==='due'){
                  // allow free text date: 支援 今天/明天/後天/本週X/下週X/月底 + 時/點/時段
                  const d = new Date();
                  const setTime = (h=9,m=0)=>{ d.setHours(h,m,0,0); };
                  let matched=false;
                  if(/無期限|無|略過/.test(text)){ st.rec.dueAt=''; matched=true; }
                  if(!matched){
                    if(/今天/.test(text)){ matched=true; }
                    else if(/明天/.test(text)){ d.setDate(d.getDate()+1); matched=true; }
                    else if(/後天/.test(text)){ d.setDate(d.getDate()+2); matched=true; }
                    else{
                      const wmap={ '日':0,'一':1,'二':2,'三':3,'四':4,'五':5,'六':6 };
                      const m1=text.match(/本週([日一二三四五六])/); const m2=text.match(/下週([日一二三四五六])/);
                      if(m1){ const t=wmap[m1[1]]; const cur=d.getDay(); const add=(t-cur+7)%7; d.setDate(d.getDate()+add); matched=true; }
                      else if(m2){ const t=wmap[m2[1]]; const cur=d.getDay(); const add=((t - cur + 7) % 7) + 7; d.setDate(d.getDate()+add); matched=true; }
                      else if(/月底/.test(text)){ d.setMonth(d.getMonth()+1, 0); matched=true; }
                    }
                  }
                  const mH = text.match(/(早上|上午|早|中午|下午|傍晚|晚上)?\s*(\d{1,2})\s*(點|時)(?:\s*(\d{1,2})\s*分)?/);
                  if(mH){
                    let h=Number(mH[2])||9; const mm=Number(mH[4])||0; const zh=String(mH[1]||'');
                    if(/下午|傍晚|晚上/.test(zh) && h<12) h+=12; if(/中午/.test(zh) && h===12) h=12; if(/早上|上午|早/.test(zh) && h===12) h=0;
                    setTime(h,mm); matched=true;
                  }else{
                    const mHM = text.match(/\b(\d{1,2}):(\d{2})\b/);
                    if(mHM){ setTime(Number(mHM[1])||9, Number(mHM[2])||0); matched=true; }
                  }
                  // ISO yyyy-mm-dd or yyyy-mm-dd hh:mm
                  const mISO = text.match(/(20\d{2}-\d{2}-\d{2})(?:[ T](\d{2}:\d{2}))?/);
                  if(mISO){ const iso = mISO[1] + (mISO[2]?`T${mISO[2]}:00`:'T00:00:00'); try{ st.rec.dueAt = new Date(iso).toISOString(); matched=true; }catch(_){ } }
                  if(!matched){ setTime(9,0); }
                  if(matched && !mISO){ try{ st.rec.dueAt = d.toISOString(); }catch(_){ st.rec.dueAt=''; } }
                  st.step='repeat'; remFlow.set(uid, st);
                  const bubble = buildRemRepeatPrompt(getBaseUrl(req)||'');
                  await lineReply(replyToken, [{ type:'flex', altText:'設定週期', contents:bubble }]);
                  continue;
                }
                if(st.step==='repeat_weekdays'){
                  const nums = text.split(/[,，\s]+/).map(s=> s.trim()).filter(Boolean).map(s=> Number(s)).filter(n=> n>=0 && n<=6);
                  st.rec.weekdays = nums;
                  st.step='priority'; remFlow.set(uid, st);
                  const bubble = buildRemPriorityPrompt(getBaseUrl(req)||'');
                  await lineReply(replyToken, [{ type:'flex', altText:'設定優先', contents:bubble }]);
                  continue;
                }
                if(st.step==='monthly_day'){
                  const n = Number(text);
                  if(Number.isFinite(n) && n>=1 && n<=31){ st.rec.monthDay = n; }
                  st.step='priority'; remFlow.set(uid, st);
                  const bubble = buildRemPriorityPrompt(getBaseUrl(req)||'');
                  await lineReply(replyToken, [{ type:'flex', altText:'設定優先', contents:bubble }]);
                  continue;
                }
                if(st.step==='priority'){
                  if(/低/.test(text)) st.rec.priority='low';
                  else if(/高/.test(text)) st.rec.priority='high';
                  else st.rec.priority='medium';
                  st.step='note'; remFlow.set(uid, st);
                  const bubble = buildRemNotePrompt(getBaseUrl(req)||'');
                  await lineReply(replyToken, [{ type:'flex', altText:'新增備註', contents:bubble }]);
                  continue;
                }
                if(st.step==='note'){
                  st.rec.note = (/略過/.test(text)) ? '' : text;
                  st.step='confirm'; remFlow.set(uid, st);
                  const bubble = buildRemConfirmBubble(getBaseUrl(req)||'', st.rec);
                  await lineReply(replyToken, [{ type:'flex', altText:'確認新增', contents:bubble }]);
                  continue;
                }
              }
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

            // 預算差額（當月或指定月）
            if(/預算/.test(text) || /差額/.test(text)){
              const month = (text.match(/(\d{4}-\d{2})/)||[])[1] || new Date().toISOString().slice(0,7);
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              try{
                const r = await callToolByName('budget_delta', { month }, uid);
                if(r && r.ok){
                  const fmt = (n)=> new Intl.NumberFormat(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}).format(n);
                  const lines = [
                    `月份：${r.month}`,
                    `預算：$${fmt(r.budgetTWD)}`,
                    `已花：$${fmt(r.spentTWD)}`,
                    `差額：$${fmt(r.deltaTWD)}（${r.status==='under'?'未超支':'超支'}）`
                  ];
                  const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req), title:'預算差額', subtitle:'月度預算與差額', lines });
                  await lineReply(replyToken, [{ type:'flex', altText:'預算差額', contents:bubble }]);
                  continue;
                }
              }catch(_){ }
            }

            // 分類排行（支出前幾名）
            if(/(分類|排行|前五|Top)/i.test(text)){
              const month = (text.match(/(\d{4}-\d{2})/)||[])[1] || new Date().toISOString().slice(0,7);
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              try{
                const r = await callToolByName('category_ranking', { month, type:'expense', top:10 }, uid);
                if(r && r.ok){
                  const fmt = (n)=> new Intl.NumberFormat(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}).format(n);
                  const lines = (r.ranking||[]).map((row,idx)=> `${idx+1}. ${row.categoryName} $${fmt(row.amountTWD)}`);
                  const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req), title:'分類排行', subtitle:`${month} 支出`, lines: lines.length?lines:['目前沒有資料'] });
                  await lineReply(replyToken, [{ type:'flex', altText:'分類排行', contents:bubble }]);
                  continue;
                }
              }catch(_){ }
            }

            // 月報（產生報告連結）
            if(/月報/.test(text)){
              const month = (text.match(/(\d{4}-\d{2})/)||[])[1] || new Date().toISOString().slice(0,7);
              const base = getBaseUrl(req)||'';
              const url = `${base.replace(/\/$/,'')}/reports/monthly?ym=${encodeURIComponent(month)}`;
              const bubble = glassFlexBubble({ baseUrl:base, title:'月報已生成', subtitle:month, lines:[ '請按下方開啟月報（HTML）' ], buttons:[ { style:'link', action:{ type:'uri', label:'開啟月報', uri:url } } ] });
              await lineReply(replyToken, [{ type:'flex', altText:'月報', contents:bubble }]);
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

            // 月曆（簡要 Flex + 快捷按鈕）
            if(/月曆/.test(text)){
              const now = new Date();
              const ym = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
              const title = '月曆檢視';
              const subtitle = ym;
              const bubble = glassFlexBubble({
                baseUrl: getBaseUrl(req)||PUBLIC_BASE_URL||'',
                title,
                subtitle,
                lines:[ '查看本月每日收入/支出概況，於網頁版可互動篩選。' ],
                buttons:[
                  { style:'secondary', color:'#64748b', action:{ type:'message', label:'最近交易', text:'最近交易' } },
                  { style:'link', action:{ type:'uri', label:'開啟月曆', uri:(getBaseUrl(req)||PUBLIC_BASE_URL||'').replace(/\/$/,'/')+'/#tab=calendar' } }
                ],
                showHero:false,
                compact:true
              });
              await lineReply(replyToken, [{ type:'flex', altText:'月曆檢視', contents:bubble }]);
              continue;
            }

            // 批次新增（說明）
            if(/批次新增/.test(text)){
              const bubble = glassFlexBubble({
                baseUrl:getBaseUrl(req)||PUBLIC_BASE_URL||'',
                title:'批次新增說明',
                subtitle:'每行一筆，支援中文金額與日期',
                lines:[
                  '格式範例：',
                  '10/5 咖啡 100',
                  '10/6 全聯 800',
                  '10/6 星巴克 500',
                  '貼上多行後直接送出即可'
                ],
                buttons:[ { style:'secondary', color:'#64748b', action:{ type:'message', label:'打開選單', text:'選單' } } ],
                showHero:false,
                compact:true
              });
              await lineReply(replyToken, [{ type:'flex', altText:'批次新增說明', contents:bubble }]);
              continue;
            }

            // AI 助理（提示）
            if(/^(AI|AI\s*助理)$/i.test(text)){
              const bubble = glassFlexBubble({
                baseUrl:getBaseUrl(req)||PUBLIC_BASE_URL||'',
                title:'AI 助理',
                subtitle:'可用自然語言記帳/查詢/修改',
                lines:[ '直接輸入：例如「支出 120 餐飲 今天 咖啡」', '或輸入「刪除上次」/「本月支出」' ],
                buttons:[ { style:'secondary', color:'#64748b', action:{ type:'message', label:'打開選單', text:'選單' } } ],
                showHero:false,
                compact:true
              });
              await lineReply(replyToken, [{ type:'flex', altText:'AI 助理', contents:bubble }]);
              continue;
            }

            // 記事：新增（訊息以「記事：內容」或「記事 內容」開頭）
            if(/^記事\s*[:：]?\s*.+/.test(text)){
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              const m = text.match(/^記事\s*[:：]?\s*(.+)$/);
              const content = (m && m[1]) ? m[1].trim() : '';
              if(content){
                try{
                  const payload = { title:'', content, tags:[], emoji:'', color:'', pinned:false, archived:false };
                  let rec;
                  if(isDbEnabled()){
                    rec = await pgdb.addNote(uid, payload);
                  } else {
                    const rows = getNotes(uid);
                    rec = { id: (crypto.randomUUID&&crypto.randomUUID())||String(Date.now()), ...payload, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
                    rows.unshift(rec); setNotes(uid, rows);
                  }
                  const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req)||PUBLIC_BASE_URL||'', title:'已新增記事', subtitle:new Date().toLocaleString('zh-TW'), lines:[ content.slice(0,80) ], showHero:false, compact:true });
                  await lineReply(replyToken, [{ type:'flex', altText:'已新增記事', contents:bubble }]);
                  continue;
                }catch(_){ /* ignore */ }
              }
              const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req)||PUBLIC_BASE_URL||'', title:'新增記事', subtitle:'請以「記事：內容」格式輸入', lines:[], showHero:false, compact:true });
              await lineReply(replyToken, [{ type:'flex', altText:'新增記事', contents:bubble }]);
              continue;
            }

            // 新增記事（引導式流程）
            if(/新增記事/.test(text)){
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              const st = { step:'content', rec:{ title:'', content:'', tags:[], emoji:'', color:'', pinned:false, archived:false } };
              noteFlow.set(uid, st);
              const bubble = buildNoteContentPrompt(getBaseUrl(req)||'');
              await lineReply(replyToken, [{ type:'flex', altText:'新增記事', contents:bubble }]);
              continue;
            }

            // 記事清單（最近 5 筆）
            if(/記事清單/.test(text)){
              try{
                const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
                let rows = [];
                if(isDbEnabled()){
                  rows = await pgdb.getNotes(uid);
                } else {
                  rows = getNotes(uid);
                }
                rows = rows.slice(0,5);
                const lines = rows.length? rows.map(n=> `${new Date(n.updatedAt||n.createdAt).toLocaleString()}｜${(n.title||'').slice(0,20)}${n.title?'：':''}${(n.content||'').slice(0,40)}`) : ['目前沒有記事'];
                const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req)||PUBLIC_BASE_URL||'', title:'記事清單（最近）', subtitle:`筆數：${rows.length}`, lines, buttons:[ { style:'link', action:{ type:'uri', label:'開啟網頁版', uri:(getBaseUrl(req)||PUBLIC_BASE_URL||'').replace(/\/$/,'/')+'/#tab=notes' } } ], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'記事清單', contents:bubble }]);
                continue;
              }catch(_){ /* ignore */ }
            }

            // 提醒：新增（訊息以「提醒：內容 [YYYY-MM-DD[ HH:MM]]」）
            if(/^提醒\s*[:：]?\s*.+/.test(text)){
              const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
              const s = text.replace(/^提醒\s*[:：]?\s*/,'');
              const m = s.match(/(\d{4}-\d{2}-\d{2})(?:[ T](\d{2}:\d{2}))?/);
              const title = m ? s.replace(m[0],'').trim() : s.trim();
              let dueAt = '';
              if(m){
                const iso = m[1] + (m[2]?`T${m[2]}:00`:'T00:00:00');
                try{ dueAt = new Date(iso).toISOString(); }catch(_){ dueAt=''; }
              }
              if(title){
                try{
                  const payload = { title, note:'', priority:'medium', tags:[], repeat:'none', monthDay:undefined, weekdays:[], done:false, dueAt };
                  let rec;
                  if(isDbEnabled()){
                    rec = await pgdb.addReminder(uid, payload);
                  } else {
                    const rows = getReminders(uid);
                    rec = { id: (crypto.randomUUID&&crypto.randomUUID())||String(Date.now()), ...payload, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
                    rows.unshift(rec); setReminders(uid, rows);
                  }
                  const line1 = dueAt ? `${new Date(dueAt).toLocaleString()}｜${title}` : title;
                  const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req)||PUBLIC_BASE_URL||'', title:'已新增提醒', subtitle:new Date().toLocaleString('zh-TW'), lines:[ line1 ], showHero:false, compact:true });
                  await lineReply(replyToken, [{ type:'flex', altText:'已新增提醒', contents:bubble }]);
                  continue;
                }catch(_){ /* ignore */ }
              }
              const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req)||PUBLIC_BASE_URL||'', title:'新增提醒', subtitle:'請以「提醒：內容 YYYY-MM-DD[ HH:MM]」輸入', lines:['日期可省略'], showHero:false, compact:true });
              await lineReply(replyToken, [{ type:'flex', altText:'新增提醒', contents:bubble }]);
              continue;
            }

            // 提醒清單（最近 5 筆）
            if(/提醒清單/.test(text)){
              try{
                const uid = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
                let rows = [];
                if(isDbEnabled()){
                  rows = await pgdb.getReminders(uid);
                } else {
                  rows = getReminders(uid);
                }
                rows = rows.slice(0,5);
                const lines = rows.length? rows.map(r=> `${r.dueAt?new Date(r.dueAt).toLocaleString():'無期限'}｜${r.title}`) : ['目前沒有提醒'];
                const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req)||PUBLIC_BASE_URL||'', title:'提醒清單（最近）', subtitle:`筆數：${rows.length}`, lines, buttons:[ { style:'link', action:{ type:'uri', label:'開啟網頁版', uri:(getBaseUrl(req)||PUBLIC_BASE_URL||'').replace(/\/$/,'/')+'/#tab=reminders' } } ], showHero:false, compact:true });
                await lineReply(replyToken, [{ type:'flex', altText:'提醒清單', contents:bubble }]);
                continue;
              }catch(_){ /* ignore */ }
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
                    const uid2 = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
                    if(seenAndMarkRecent(uid2, payload)) { skipped++; continue; }
                    if(isDbEnabled()) await pgdb.addTransaction(userId, payload);
                    else { fileStore.addTransaction(uid2, payload); }
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
              {
                const uid3 = userId || (lineUidRaw ? `line:${lineUidRaw}` : 'anonymous');
                if(!seenAndMarkRecent(uid3, payload)){
                  if(isDbEnabled()){
                    await pgdb.addTransaction(userId, payload);
                  } else {
                    try{ fileStore.addTransaction(uid3, payload); }catch(_){ }
                  }
                }
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
            // Fallback: chat with AI（改用同一套工具/檢索管線）
            {
              const ctx = { transactions: await (isDbEnabled()? pgdb.getTransactions(userId) : fileStore.getTransactions(userId||'anonymous')), categories: await (isDbEnabled()? pgdb.getCategories() : fileStore.getCategories(userId||'anonymous')), settings: await (isDbEnabled()? pgdb.getSettings(userId) : fileStore.getSettings?.(userId||'anonymous')) };
              try{
                const aiUrl = `http://127.0.0.1:${PORT}/api/ai`;
                const data = await fetchJson(aiUrl, { method:'POST', headers:{ 'Content-Type':'application/json' }, body: JSON.stringify({ messages:[{ role:'user', content:text }], context: ctx, mode:'chat' }) }, 20000);
                const replyText = data?.reply || '';
                await lineReply(replyToken, [{ type:'text', text: String(replyText||'') }]);
              }catch(_){
                const replyText = heuristicReply([{ role:'user', content:text }], ctx) || '（AI 暫時不可用）';
                await lineReply(replyToken, [{ type:'text', text: String(replyText||'') }]);
              }
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

    // Agents triage demo endpoint
    if (req.method === 'POST' && reqPath === '/api/ai/agents/triage'){
      const raw = await parseBody(req);
      const { input='' } = JSON.parse(raw.toString('utf-8')||'{}');
      const r = await aiAgentsTriage(input);
      res.writeHead(r.ok?200:502, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify(r));
    }

    // OpenAI Responses API (text)
    if (req.method === 'POST' && reqPath === '/api/ai/responses/text'){
      const raw = await parseBody(req);
      const { input='' } = JSON.parse(raw.toString('utf-8')||'{}');
      const r = await aiResponsesText(input);
      if(!r.ok){
        const fb = heuristicReply([{ role:'user', content:String(input||'') }], {});
        res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, output:String(fb||'') }));
      }
      res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify(r));
    }

    // OpenAI Responses API (vision)
    if (req.method === 'POST' && reqPath === '/api/ai/responses/vision'){
      const raw = await parseBody(req);
      const { text='', imageUrl='' } = JSON.parse(raw.toString('utf-8')||'{}');
      const r = await aiResponsesVision(text, imageUrl);
      if(!r.ok){
        const fb = heuristicReply([{ role:'user', content:String(text||'') }], {});
        res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, output:String(fb||'') }));
      }
      res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify(r));
    }

    // OpenAI Responses API (web_search)
    if (req.method === 'POST' && reqPath === '/api/ai/responses/web_search'){
      const raw = await parseBody(req);
      const { input='' } = JSON.parse(raw.toString('utf-8')||'{}');
      const r = await aiResponsesWebSearch(input);
      if(!r.ok){
        const fb = heuristicReply([{ role:'user', content:String(input||'') }], {});
        res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, output:String(fb||'') }));
      }
      res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify(r));
    }

    // OpenAI Responses API (file_search)
    if (req.method === 'POST' && reqPath === '/api/ai/responses/file_search'){
      const raw = await parseBody(req);
      const { input='' } = JSON.parse(raw.toString('utf-8')||'{}');
      const r = await aiResponsesFileSearch(input);
      if(!r.ok){
        const fb = heuristicReply([{ role:'user', content:String(input||'') }], {});
        res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, output:String(fb||'') }));
      }
      res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify(r));
    }

    // OpenAI Responses API (function tool)
    if (req.method === 'POST' && reqPath === '/api/ai/responses/function_tool'){
      const raw = await parseBody(req);
      const { input='' } = JSON.parse(raw.toString('utf-8')||'{}');
      const r = await aiResponsesFunctionTool(input);
      if(!r.ok){
        const fb = heuristicReply([{ role:'user', content:String(input||'') }], {});
        res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, output:String(fb||'') }));
      }
      res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify(r));
    }

    // OpenAI Responses API (MCP)
    if (req.method === 'POST' && reqPath === '/api/ai/responses/mcp'){
      const raw = await parseBody(req);
      const { input='' } = JSON.parse(raw.toString('utf-8')||'{}');
      const r = await aiResponsesMcp(input);
      if(!r.ok){
        const fb = heuristicReply([{ role:'user', content:String(input||'') }], {});
        res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, output:String(fb||'') }));
      }
      res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify(r));
    }

    // OpenAI Responses API (stream aggregated)
    if (req.method === 'POST' && reqPath === '/api/ai/responses/stream'){
      const raw = await parseBody(req);
      const { input='' } = JSON.parse(raw.toString('utf-8')||'{}');
      const r = await aiResponsesStreamOnce(input);
      if(!r.ok){
        const fb = heuristicReply([{ role:'user', content:String(input||'') }], {});
        res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, output:String(fb||'') }));
      }
      res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
      return res.end(JSON.stringify(r));
    }

  // Notes CRUD
  if(reqPath==='/api/notes' && req.method==='GET'){
    const user = getUserFromRequest(req);
    let uid = user?.id || '';
    if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
    if(!uid) uid = 'anonymous';
    const commonHeaders = { 'Content-Type':'application/json; charset=utf-8', 'Cache-Control':'no-store', 'X-UID': uid, 'X-User-Mode': isDbEnabled()?'db':'file' };
    if(isDbEnabled()){
      const rows = await pgdb.getNotes(uid);
      res.writeHead(200, commonHeaders);
      return res.end(JSON.stringify(rows));
    } else {
      res.writeHead(200, commonHeaders);
      return res.end(JSON.stringify(getNotes(uid)));
    }
  }
  if(reqPath==='/api/notes' && req.method==='POST'){
    const user = getUserFromRequest(req);
    let uid = user?.id || '';
    if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
    if(!uid) uid = 'anonymous';
    const raw = await parseBody(req); const body = JSON.parse(raw.toString('utf-8')||'{}');
    const payload = {
      title: String(body.title||'').slice(0,120),
      content: String(body.content||'').slice(0,4000),
      tags: Array.isArray(body.tags) ? body.tags.slice(0,20).map(x=>String(x).slice(0,24)) : [],
      pinned: !!body.pinned,
      color: String(body.color||'').slice(0,16), // 'red','blue','green','yellow','purple'...
      emoji: String(body.emoji||'').slice(0,4),
      archived: !!body.archived
    };
    if(isDbEnabled()){
      const rec = await pgdb.addNote(uid, payload);
      setTimeout(()=>{ upsertNoteVector(uid, rec); }, 0);
      res.writeHead(200,{ 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:true, note:rec }));
    } else {
      const rows = getNotes(uid);
      const rec = {
        id: safeId(),
        ...payload,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
      };
      rows.unshift(rec); setNotes(uid, rows); setTimeout(()=>{ upsertNoteVector(uid, rec); }, 0);
      res.writeHead(200,{ 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:true, note:rec }));
    }
  }
  if(/^\/api\/notes\//.test(reqPath) && req.method==='PUT'){
    const id = decodeURIComponent(reqPath.split('/').pop()||''); const user = getUserFromRequest(req); let uid = user?.id || '';
    if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
    if(!uid) uid = 'anonymous';
    const raw = await parseBody(req); const body = JSON.parse(raw.toString('utf-8')||'{}');
    const patch = {
      title: String((body.title||'')||'').slice(0,120),
      content: String((body.content||'')||'').slice(0,4000),
      tags: Array.isArray(body.tags) ? body.tags.slice(0,20).map(x=>String(x).slice(0,24)) : undefined,
      color: String((body.color||'')||'').slice(0,16),
      emoji: String((body.emoji||'')||'').slice(0,4),
      pinned: typeof body.pinned === 'boolean' ? body.pinned : undefined,
      archived: typeof body.archived === 'boolean' ? body.archived : undefined
    };
    // Remove undefined values
    Object.keys(patch).forEach(key => patch[key] === undefined && delete patch[key]);
    if(isDbEnabled()){
      const next = await pgdb.updateNote(uid, id, patch);
      if(!next){ res.writeHead(404,{ 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); }
      setTimeout(()=>{ upsertNoteVector(uid, next); }, 0);
      res.writeHead(200,{ 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:true, note:next }));
    } else {
      const rows = getNotes(uid); const idx = rows.findIndex(n=>n.id===id); if(idx<0){ res.writeHead(404,{ 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); }
      const next = {
        ...rows[idx],
        ...patch,
        updatedAt: new Date().toISOString()
      };
      rows[idx]=next; setNotes(uid, rows); setTimeout(()=>{ upsertNoteVector(uid, next); }, 0);
      res.writeHead(200,{ 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:true, note:next }));
    }
  }
  if(/^\/api\/notes\//.test(reqPath) && req.method==='DELETE'){
    const id = decodeURIComponent(reqPath.split('/').pop()||''); const user = getUserFromRequest(req); let uid = user?.id || '';
    if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
    if(!uid) uid = 'anonymous';
    if(isDbEnabled()){
      await pgdb.deleteNote(uid, id);
      res.writeHead(200,{ 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:true }));
    } else {
      const rows = getNotes(uid).filter(n=>n.id!==id); setNotes(uid, rows); setTimeout(()=>{ deleteNoteEmbeddingFile(uid, id); }, 0);
      res.writeHead(200,{ 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:true }));
    }
  }

  // Reminders CRUD
  if(reqPath==='/api/reminders' && req.method==='GET'){
    const user = getUserFromRequest(req);
    let uid = user?.id || '';
    if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
    if(!uid) uid = 'anonymous';
    const commonHeaders = { 'Content-Type':'application/json; charset=utf-8', 'Cache-Control':'no-store', 'X-UID': uid, 'X-User-Mode': isDbEnabled()?'db':'file' };
    if(isDbEnabled()){
      const rows = await pgdb.getReminders(uid);
      res.writeHead(200, commonHeaders);
      return res.end(JSON.stringify(rows));
    } else {
      res.writeHead(200, commonHeaders);
      return res.end(JSON.stringify(getReminders(uid)));
    }
  }
  if(reqPath==='/api/reminders' && req.method==='POST'){
    const user = getUserFromRequest(req);
    let uid = user?.id || '';
    if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
    if(!uid) uid = 'anonymous';
    const raw = await parseBody(req); const body = JSON.parse(raw.toString('utf-8')||'{}');
    const payload = {
      title: String(body.title||'').slice(0,160),
      dueAt: String(body.dueAt||''),
      repeat: String(body.repeat||'none'), // none|daily|weekly|monthly
      weekdays: Array.isArray(body.weekdays) ? body.weekdays.map(n=> Number(n)|0).filter(n=> n>=0 && n<=6).slice(0,7) : [],
      monthDay: Number.isFinite(Number(body.monthDay)) ? Math.max(1, Math.min(31, Number(body.monthDay))) : undefined,
      priority: ['low','medium','high'].includes(String(body.priority)) ? String(body.priority) : 'medium',
      tags: Array.isArray(body.tags) ? body.tags.slice(0,20).map(x=>String(x).slice(0,24)) : [],
      note: String(body.note||'').slice(0,1000),
      done: !!body.done
    };
    if(isDbEnabled()){
      const rec = await pgdb.addReminder(uid, payload);
      res.writeHead(200,{ 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:true, reminder:rec }));
    } else {
      const rows = getReminders(uid);
      const rec = {
        id: safeId(),
        ...payload,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
      };
      rows.unshift(rec); setReminders(uid, rows);
      res.writeHead(200,{ 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:true, reminder:rec }));
    }
  }
  if(/^\/api\/reminders\//.test(reqPath) && req.method==='PUT'){
    const id = decodeURIComponent(reqPath.split('/').pop()||''); const user = getUserFromRequest(req); let uid = user?.id || '';
    if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
    if(!uid) uid = 'anonymous';
    const raw = await parseBody(req); const body = JSON.parse(raw.toString('utf-8')||'{}');
    const patch = {
      title: String((body.title||'')||'').slice(0,160),
      dueAt: String(body.dueAt||''),
      repeat: String((body.repeat||'')||'none'),
      weekdays: Array.isArray(body.weekdays) ? body.weekdays.map(n=> Number(n)|0).filter(n=> n>=0 && n<=6).slice(0,7) : undefined,
      monthDay: Number.isFinite(Number(body.monthDay)) ? Math.max(1, Math.min(31, Number(body.monthDay))) : undefined,
      priority: ['low','medium','high'].includes(String(body.priority)) ? String(body.priority) : undefined,
      tags: Array.isArray(body.tags) ? body.tags.slice(0,20).map(x=>String(x).slice(0,24)) : undefined,
      note: String((body.note||'')||'').slice(0,1000),
      done: typeof body.done === 'boolean' ? body.done : undefined
    };
    // Remove undefined values
    Object.keys(patch).forEach(key => patch[key] === undefined && delete patch[key]);
    if(isDbEnabled()){
      const next = await pgdb.updateReminder(uid, id, patch);
      if(!next){ res.writeHead(404,{ 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); }
      res.writeHead(200,{ 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:true, reminder:next }));
    } else {
      const rows = getReminders(uid); const idx = rows.findIndex(n=>n.id===id); if(idx<0){ res.writeHead(404,{ 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); }
      const next = {
        ...rows[idx],
        ...patch,
        updatedAt: new Date().toISOString()
      };
      rows[idx]=next; setReminders(uid, rows);
      res.writeHead(200,{ 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:true, reminder:next }));
    }
  }
  if(/^\/api\/reminders\//.test(reqPath) && req.method==='DELETE'){
    const id = decodeURIComponent(reqPath.split('/').pop()||''); const user = getUserFromRequest(req); let uid = user?.id || '';
    if(!uid && !REQUIRE_AUTH){ uid = ensureAnonCookie(req, res) || 'anonymous'; }
    if(!uid) uid = 'anonymous';
    if(isDbEnabled()){
      await pgdb.deleteReminder(uid, id);
      res.writeHead(200,{ 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:true }));
    } else {
      const rows = getReminders(uid).filter(n=>n.id!==id); setReminders(uid, rows);
      res.writeHead(200,{ 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:true }));
    }
  }

    res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
    return res.end(JSON.stringify({ ok: false, error: 'not_found', path: reqPath, method: req.method }));
  } catch (err) {
    console.error(err);
    res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
    return res.end(JSON.stringify({ ok: false, error: 'server_error' }));
  }

  // Nudges API
  if (req.method === 'GET' && reqPath === '/api/nudges/next'){
    const user = REQUIRE_AUTH ? getUserFromRequest(req) : null;
    const userId = REQUIRE_AUTH ? (user?.id || user?.userId || null) : null;
    const list = await computeNudges(userId);
    res.writeHead(200, { 'Content-Type':'application/json; charset=utf-8' });
    return res.end(JSON.stringify({ ok:true, nudges:list }));
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
    const isHttp = u.protocol === 'http:';
    const transport = isHttp ? http : https;
    const req = transport.request({
      hostname: u.hostname,
      port: u.port || (isHttp ? 80 : 443),
      path: `${u.pathname}${u.search}`,
      method: opts?.method || 'GET',
      headers: opts?.headers || {}
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
