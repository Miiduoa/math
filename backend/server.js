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
  deleteTransaction(userId, id){ let rows=this.getTransactions(userId); const before=rows.length; rows=rows.filter(t=>t.id!==id); writeJson(path.join(userDirFor(userId),'transactions.json'), rows); return rows.length<before; },
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
    const sid = verifySigned(raw);
    if(sid){
      const s = sessions.get(sid);
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
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Line-Signature');
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
  if(/(^|\s)(支出|花費|付|花|扣)($|\s)/.test(t)) result.type='expense';
  if(/(^|\s)(收入|入帳|收)($|\s)/.test(t)) result.type='income';
  const amt = t.match(/([0-9]+(?:\.[0-9]+)?)/);
  if(amt) result.amount = Number(amt[1]);
  const cur = t.match(/\b(TWD|USD|JPY|EUR|CNY|HKD)\b/i);
  if(cur) result.currency = cur[1].toUpperCase();
  const rate = t.match(/匯率\s*([0-9]+(?:\.[0-9]+)?)/);
  if(rate) result.rate = Number(rate[1]);
  const date = t.match(/(20\d{2})[-\/](\d{1,2})[-\/](\d{1,2})/);
  if(date){ const y=Number(date[1]); const m=String(Number(date[2])).padStart(2,'0'); const d=String(Number(date[3])).padStart(2,'0'); result.date=`${y}-${m}-${d}`; }
  const camt = t.match(/請款\s*([0-9]+(?:\.[0-9]+)?)/);
  if(camt) result.claimAmount = Number(camt[1]);
  if(/已請款|完成請款|報帳完成/.test(t)) result.claimed = true;
  if(/未請款|還沒請款/.test(t)) result.claimed = false;
  result.note = t;
  return result;
}

async function aiStructParse(text, context){
  const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';
  const OPENAI_BASE_URL = process.env.OPENAI_BASE_URL || 'https://api.openai.com/v1';
  const OPENAI_MODEL = process.env.OPENAI_MODEL || 'gpt-4o-mini';
  if(!OPENAI_API_KEY){ return null; }
  const payload = {
    model: OPENAI_MODEL,
    messages: [
      { role:'system', content:'你是一個記帳解析器，請輸出 JSON，包含: type(income|expense), amount(number), currency(string), date(YYYY-MM-DD), categoryName(string), rate(number,可省略), claimAmount(number,可省略), claimed(boolean,可省略), note(string)。只輸出 JSON，不要其他文字。'},
      { role:'user', content: text }
    ],
    temperature: 0.2
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
      const sid = crypto.randomBytes(16).toString('hex');
      sessions.set(sid, { user, createdAt: Date.now() });
      // 在本機開發（http）時不要加 Secure，否則瀏覽器不會儲存 cookie
      const isHttps = /^https:\/\//.test(getBaseUrl(req)||'');
      setCookie(res, 'session', createSigned(sid), { maxAge: 60*60*24*7, secure: isHttps });
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
      const cookies = parseCookies(req);
      const raw = cookies['session']||'';
      const sid = verifySigned(raw);
      if(sid){ sessions.delete(sid); }
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
          { role: 'system', content: '你是一個記帳解析器，請輸出 JSON，包含: type(income|expense), amount(number), currency(string), date(YYYY-MM-DD), categoryName(string), rate(number，可省略), claimAmount(number，可省略), claimed(boolean，可省略), note(string)。只輸出 JSON，不要其他文字。' },
          { role: 'user', content: messages?.[0]?.content || '' }
        ] : [
          { role: 'system', content: 'You are a helpful finance and budgeting assistant for a personal ledger web app. Answer in Traditional Chinese.' },
          { role: 'system', content: `Context JSON (may be partial): ${JSON.stringify(context).slice(0, 4000)}` },
          ...messages
        ],
        temperature: 0.4
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
            const json = JSON.parse(reply);
            res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
            return res.end(JSON.stringify({ ok:true, provider:'openai', parsed: json }));
          }catch(_){ /* fallthrough to plain text */ }
        }
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok: true, provider: 'openai', reply }));
      }catch(err){
        res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:false, error:'ai_provider', detail: String(err?.message||err) }));
      }
    }

    if (req.method === 'GET' && reqPath === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      return res.end(JSON.stringify({ ok: true }));
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
            // Try AI struct parse first
            let parsed = await aiStructParse(text, {});
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
            // Fallback help
            {
              const lines = [
                '記帳：支出 120 餐飲 早餐 2025-10-12',
                '查詢：這月支出多少？',
                '清單：最近交易、未請款、分類支出',
                '功能：輸入「選單」開啟功能選單'
              ];
              const bubble = glassFlexBubble({ baseUrl:getBaseUrl(req), title:'可用指令', subtitle:'也支援自然語言', lines });
              await lineReply(replyToken, [{ type:'flex', altText:'使用說明', contents:bubble }]);
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


