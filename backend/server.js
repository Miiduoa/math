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
    if(!sid) return null;
    const s = sessions.get(sid);
    return s && s.user ? s.user : null;
  }catch(_){ return null; }
}

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Line-Signature');
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
  const hmac = crypto.createHmac('sha256', LINE_CHANNEL_SECRET);
  hmac.update(rawBody);
  const digest = hmac.digest('base64');
  return crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(digest));
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

    // LINE Login start
    if (req.method === 'GET' && reqPath === '/auth/line/start'){
      const state = crypto.randomBytes(12).toString('hex');
      const nonce = crypto.randomBytes(12).toString('hex');
      setCookie(res, 'linestate', createSigned(`${state}|${nonce}|${Date.now()}`), { maxAge: 600 });
      const authz = new URL('https://access.line.me/oauth2/v2.1/authorize');
      authz.searchParams.set('response_type','code');
      authz.searchParams.set('client_id', LINE_LOGIN_CHANNEL_ID);
      authz.searchParams.set('redirect_uri', LINE_LOGIN_REDIRECT_URI);
      authz.searchParams.set('state', state);
      authz.searchParams.set('scope','profile openid');
      authz.searchParams.set('nonce', nonce);
      res.writeHead(302, { Location: authz.toString() });
      return res.end();
    }
    // LINE Login callback
    if (req.method === 'GET' && reqPath === '/auth/line/callback'){
      const code = url.searchParams.get('code')||'';
      const state = url.searchParams.get('state')||'';
      const cookies = parseCookies(req);
      const raw = cookies['linestate']||'';
      const parsed = verifySigned(raw);
      if(!parsed){ res.writeHead(400, { 'Content-Type':'text/plain; charset=utf-8' }); return res.end('Invalid state'); }
      const [savedState] = parsed.split('|');
      if(!state || state!==savedState){ res.writeHead(400, { 'Content-Type':'text/plain; charset=utf-8' }); return res.end('State mismatch'); }
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
      }catch(err){ res.writeHead(502, { 'Content-Type':'text/plain; charset=utf-8' }); return res.end('Token exchange failed'); }
      const accessToken = tokenData?.access_token||'';
      // fetch profile
      let profile=null;
      try{
        profile = await fetchJson('https://api.line.me/v2/profile', { headers:{ 'Authorization': `Bearer ${accessToken}` } }, 15000);
      }catch(_){ profile=null; }
      const user = {
        id: profile?.userId || 'line:'+(tokenData?.id_token||'').slice(0,8),
        name: profile?.displayName || 'LINE User',
        picture: profile?.pictureUrl || ''
      };
      const sid = crypto.randomBytes(16).toString('hex');
      sessions.set(sid, { user, createdAt: Date.now() });
      setCookie(res, 'session', createSigned(sid), { maxAge: 60*60*24*7 });
      clearCookie(res, 'linestate');
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
    if (req.method === 'GET' && (reqPath === '/' || reqPath === '/index.html' || reqPath === '/styles.css' || reqPath === '/app.js' || reqPath === '/db.js')) {
      const __filename = fileURLToPath(import.meta.url);
      const __dirname = path.dirname(__filename);
      const rootDir = path.resolve(__dirname, '..');
      const filePath = reqPath === '/' ? path.join(rootDir, 'index.html') : path.join(rootDir, reqPath.slice(1));
      try{
        const ext = reqPath === '/' ? '.html' : path.extname(filePath);
        const mime = ext === '.html' ? 'text/html; charset=utf-8'
          : ext === '.css' ? 'text/css; charset=utf-8'
          : ext === '.js' ? 'application/javascript; charset=utf-8'
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
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(store.categories));
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
        const id = String(name).trim().toLowerCase().replace(/\s+/g,'-');
        if(!id){ res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false, error:'invalid_name' })); }
        if(store.categories.some(c=>c.id===id)){ res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:true, category: store.categories.find(c=>c.id===id) })); }
        const cat = { id, name: String(name).trim() };
        store.categories.push(cat);
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
        if(store.transactions.some(t=>t.categoryId===id)){
          res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
          return res.end(JSON.stringify({ ok:false, error:'in_use' }));
        }
        const before = store.categories.length;
        store.categories = store.categories.filter(c=>c.id!==id);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok: store.categories.length<before }));
      }
    }

    // Transactions
    if (req.method === 'GET' && reqPath === '/api/transactions'){
      if(REQUIRE_AUTH){ const user = getUserFromRequest(req); if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify([])); } }
      const user = REQUIRE_AUTH ? getUserFromRequest(req) : null;
      if(isDbEnabled()){
        const rows = await pgdb.getTransactions(user?.id);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(rows));
      } else {
        const all = store.transactions;
        const rows = user ? all.filter(t=>t.userId===user.id) : all;
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(rows));
      }
    }
    if (req.method === 'POST' && reqPath === '/api/transactions'){
      if(REQUIRE_AUTH){ const user = getUserFromRequest(req); if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false, error:'unauthorized' })); } }
      const raw = await parseBody(req);
      const payload = JSON.parse(raw.toString('utf-8') || '{}');
      const user = REQUIRE_AUTH ? getUserFromRequest(req) : null;
      if(isDbEnabled()){
        const rec = await pgdb.addTransaction(user?.id, payload);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      } else {
        const id = (crypto.randomUUID && crypto.randomUUID()) || String(Date.now())+Math.random().toString(16).slice(2);
        const rec = { id, userId: user?.id, ...payload };
        store.transactions.unshift(rec);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      }
    }
    if (req.method === 'GET' && reqPath.startsWith('/api/transactions/')){
      if(REQUIRE_AUTH){ const user = getUserFromRequest(req); if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const id = decodeURIComponent(reqPath.split('/').pop()||'');
      const user = REQUIRE_AUTH ? getUserFromRequest(req) : null;
      if(isDbEnabled()){
        const rec = await pgdb.getTransactionById(user?.id, id);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      } else {
        const rec = store.transactions.find(t=>t.id===id && (!user || t.userId===user.id)) || null;
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      }
    }
    if (req.method === 'PUT' && reqPath.startsWith('/api/transactions/')){
      if(REQUIRE_AUTH){ const user = getUserFromRequest(req); if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const id = decodeURIComponent(reqPath.split('/').pop()||'');
      const raw = await parseBody(req);
      const patch = JSON.parse(raw.toString('utf-8') || '{}');
      const user = REQUIRE_AUTH ? getUserFromRequest(req) : null;
      if(isDbEnabled()){
        const rec = await pgdb.updateTransaction(user?.id, id, patch);
        if(!rec){ res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); }
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, transaction: rec }));
      } else {
        const idx = store.transactions.findIndex(t=>t.id===id && (!user || t.userId===user.id));
        if(idx<0){ res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); }
        store.transactions[idx] = { ...store.transactions[idx], ...patch };
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, transaction: store.transactions[idx] }));
      }
    }
    if (req.method === 'DELETE' && reqPath.startsWith('/api/transactions/')){
      if(REQUIRE_AUTH){ const user = getUserFromRequest(req); if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const id = decodeURIComponent(reqPath.split('/').pop()||'');
      const user = REQUIRE_AUTH ? getUserFromRequest(req) : null;
      if(isDbEnabled()){
        await pgdb.deleteTransaction(user?.id, id);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true }));
      } else {
        const before = store.transactions.length;
        store.transactions = store.transactions.filter(t=>!(t.id===id && (!user || t.userId===user.id)));
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok: store.transactions.length<before }));
      }
    }

    // Settings
    if (req.method === 'GET' && reqPath === '/api/settings'){
      if(REQUIRE_AUTH){ const user = getUserFromRequest(req); if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({})); } }
      const user = REQUIRE_AUTH ? getUserFromRequest(req) : null;
      if(isDbEnabled()){
        const s = await pgdb.getSettings(user?.id);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(s));
      } else {
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(store.settings));
      }
    }
    if (req.method === 'PUT' && reqPath === '/api/settings'){
      if(REQUIRE_AUTH){ const user = getUserFromRequest(req); if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({})); } }
      const raw = await parseBody(req);
      const patch = JSON.parse(raw.toString('utf-8') || '{}');
      const user = REQUIRE_AUTH ? getUserFromRequest(req) : null;
      if(isDbEnabled()){
        const next = await pgdb.setSettings(user?.id, patch);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(next));
      } else {
        store.settings = { ...store.settings, ...patch };
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(store.settings));
      }
    }

    // Model
    if (req.method === 'POST' && reqPath === '/api/model/update'){
      if(REQUIRE_AUTH){ const user = getUserFromRequest(req); if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const raw = await parseBody(req);
      const { note='', categoryId='' } = JSON.parse(raw.toString('utf-8')||'{}');
      const user = REQUIRE_AUTH ? getUserFromRequest(req) : null;
      if(isDbEnabled()){
        await pgdb.updateCategoryModelFromNote(user?.id, note, categoryId);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true }));
      } else {
        const words = String(note).toLowerCase().split(/[^\p{L}\p{N}]+/u).filter(Boolean);
        for(const w of words){
          let rec = store.model.find(r=>r.userId===(user&&user.id) && r.word===w);
          if(!rec){ rec={ word:w, counts:{} }; store.model.push(rec); }
          rec.counts[categoryId] = (rec.counts[categoryId]||0) + 1;
        }
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true }));
      }
    }
    if (req.method === 'POST' && reqPath === '/api/model/suggest'){
      if(REQUIRE_AUTH){ const user = getUserFromRequest(req); if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const raw = await parseBody(req);
      const { note='' } = JSON.parse(raw.toString('utf-8')||'{}');
      const user = REQUIRE_AUTH ? getUserFromRequest(req) : null;
      if(isDbEnabled()){
        const catId = await pgdb.suggestCategoryFromNote(user?.id, note);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, categoryId: catId }));
      } else {
        const words = String(note).toLowerCase().split(/[^\p{L}\p{N}]+/u).filter(Boolean);
        const scores = {};
        for(const w of words){
          const rec = store.model.find(r=>r.userId===(user&&user.id) && r.word===w);
          if(rec && rec.counts){ for(const [k,v] of Object.entries(rec.counts)){ scores[k]=(scores[k]||0)+Number(v||0);} }
        }
        let best=null,bestScore=0; for(const [k,v] of Object.entries(scores)){ if(v>bestScore){ bestScore=v; best=k; } }
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok:true, categoryId: best }));
      }
    }

    if (req.method === 'GET' && reqPath === '/api/sync/export') {
      if(REQUIRE_AUTH){ const user = getUserFromRequest(req); if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const user = REQUIRE_AUTH ? getUserFromRequest(req) : null;
      if(isDbEnabled()){
        const data = await pgdb.exportAll(user?.id);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(data));
      } else {
        const filtered = {
          categories: store.categories,
          transactions: store.transactions.filter(t=>!user || t.userId===user.id),
          settings: store.settings,
          model: store.model.filter(r=>!user || r.userId===user.id)
        };
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify(filtered));
      }
    }

    if (req.method === 'POST' && reqPath === '/api/sync/import') {
      if(REQUIRE_AUTH){ const user = getUserFromRequest(req); if(!user){ res.writeHead(401, { 'Content-Type':'application/json; charset=utf-8' }); return res.end(JSON.stringify({ ok:false })); } }
      const raw = await parseBody(req);
      const data = JSON.parse(raw.toString('utf-8') || '{}');
      if (!data || !Array.isArray(data.categories) || !Array.isArray(data.transactions)) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok: false, error: 'invalid payload' }));
      }
      const user = REQUIRE_AUTH ? getUserFromRequest(req) : null;
      if(isDbEnabled()){
        await pgdb.importAll(user?.id, data);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok: true }));
      } else {
        store.categories = data.categories;
        const uid = user && user.id;
        store.transactions = data.transactions.map(t=> ({ userId: uid, ...t }));
        if (data.settings) store.settings = data.settings;
        if (Array.isArray(data.model)) store.model = data.model.map(m=> ({ userId: uid, ...m }));
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok: true }));
      }
    }

    if (req.method === 'POST' && reqPath === '/line/webhook') {
      const raw = await parseBody(req);
      const sig = req.headers['x-line-signature'];
      if (!verifyLineSignature(raw, typeof sig === 'string' ? sig : '')) {
        res.writeHead(401, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ ok: false, error: 'signature invalid' }));
      }
      let body = {};
      try { body = JSON.parse(raw.toString('utf-8') || '{}'); } catch {}
      // Minimal echo-like handling (no reply token usage here; just acknowledge)
      const events = Array.isArray(body.events) ? body.events : [];
      console.log('LINE events:', events.map(e => ({ type: e.type, text: e.message?.text })).slice(0, 3));
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      return res.end(JSON.stringify({ ok: true }));
    }

    res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
    return res.end(JSON.stringify({ ok: false, error: 'not_found' }));
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


