(function(){
  const defaultCategories = [
    { id: 'food', name: '餐飲' },
    { id: 'transport', name: '交通' },
    { id: 'shopping', name: '購物' },
    { id: 'salary', name: '薪資' }
  ];

  const inMemory = {
    categories: [...defaultCategories],
    transactions: []
  };

  function delay(value){
    return new Promise(resolve => setTimeout(() => resolve(value), 0));
  }

  const supportsIndexedDB = typeof indexedDB !== 'undefined';
  const DB_NAME = 'ledger-db';
  const DB_VERSION = 2;
  const STORE_CATEGORIES = 'categories';
  const STORE_TX = 'transactions';
  const STORE_SETTINGS = 'settings'; // keyPath: key, value example { key:'app', baseCurrency:'TWD', monthlyBudgetTWD:0, savingsGoalTWD:0 }
  const STORE_MODEL = 'category_keywords'; // keyPath: word, value example { word:'早餐', counts: { food:10, transport:1 } }
  let db = null;

  function openDB(){
    return new Promise((resolve, reject)=>{
      const req = indexedDB.open(DB_NAME, DB_VERSION);
      req.onupgradeneeded = (ev)=>{
        const database = ev.target.result;
        if(!database.objectStoreNames.contains(STORE_CATEGORIES)){
          const cat = database.createObjectStore(STORE_CATEGORIES, { keyPath: 'id' });
        }
        if(!database.objectStoreNames.contains(STORE_TX)){
          const tx = database.createObjectStore(STORE_TX, { keyPath: 'id' });
          tx.createIndex('by_date', 'date');
          tx.createIndex('by_category', 'categoryId');
          tx.createIndex('by_type', 'type');
        }
        if(!database.objectStoreNames.contains(STORE_SETTINGS)){
          database.createObjectStore(STORE_SETTINGS, { keyPath: 'key' });
        }
        if(!database.objectStoreNames.contains(STORE_MODEL)){
          database.createObjectStore(STORE_MODEL, { keyPath: 'word' });
        }
      };
      req.onsuccess = ()=>resolve(req.result);
      req.onerror = ()=>reject(req.error);
    });
  }

  async function ensureSeeded(database){
    const tx = database.transaction([STORE_CATEGORIES, STORE_SETTINGS], 'readwrite');
    const store = tx.objectStore(STORE_CATEGORIES);
    const countReq = store.count();
    const count = await new Promise((resolve,reject)=>{
      countReq.onsuccess = ()=>resolve(countReq.result);
      countReq.onerror = ()=>reject(countReq.error);
    });
    if(count === 0){
      for(const c of defaultCategories){
        store.put(c);
      }
    }
    // default app settings if missing
    const sStore = tx.objectStore(STORE_SETTINGS);
    const getApp = sStore.get('app');
    const exists = await new Promise((resolve)=>{
      getApp.onsuccess = ()=>resolve(!!getApp.result);
      getApp.onerror = ()=>resolve(false);
    });
    if(!exists){
      sStore.put({ key:'app', baseCurrency:'TWD', monthlyBudgetTWD:0, savingsGoalTWD:0, nudges:true, appearance:'system', serverUrl:'', categoryBudgets:{} });
    }
    return new Promise((resolve,reject)=>{
      tx.oncomplete = ()=>resolve(true);
      tx.onerror = ()=>reject(tx.error);
      tx.onabort = ()=>reject(tx.error);
    });
  }

  const idb = {
    async init(){
      try{
        db = await openDB();
        await ensureSeeded(db);
        return true;
      }catch(err){
        try{ console.warn && console.warn('IndexedDB 初始化失敗，改用記憶體模式', err); }catch(_){ }
        // Fallback to memory implementation to keep the app interactive
        window.DB = memory;
        return memory.init();
      }
    },
    async getCategories(){
      const tx = db.transaction([STORE_CATEGORIES],'readonly');
      const store = tx.objectStore(STORE_CATEGORIES);
      const req = store.getAll();
      const rows = await new Promise((resolve,reject)=>{
        req.onsuccess = ()=>resolve(req.result||[]);
        req.onerror = ()=>reject(req.error);
      });
      await new Promise((r)=>{tx.oncomplete=r});
      return rows;
    },
    async addCategory(name){
      const id = name.trim().toLowerCase().replace(/\s+/g,'-');
      if(!id) return null;
      const existing = await this.getCategories();
      if(existing.some(c=>c.id===id)) return null;
      const record = { id, name: name.trim() };
      const tx = db.transaction([STORE_CATEGORIES],'readwrite');
      tx.objectStore(STORE_CATEGORIES).put(record);
      await new Promise((resolve,reject)=>{
        tx.oncomplete=()=>resolve(true);
        tx.onerror=()=>reject(tx.error);
        tx.onabort=()=>reject(tx.error);
      });
      return record;
    },
    async deleteCategory(id){
      // disallow delete if used by any transaction
      const used = await this.getTransactions();
      if(used.some(t=>t.categoryId===id)) return false;
      const tx = db.transaction([STORE_CATEGORIES],'readwrite');
      tx.objectStore(STORE_CATEGORIES).delete(id);
      await new Promise((resolve,reject)=>{
        tx.oncomplete=()=>resolve(true);
        tx.onerror=()=>reject(tx.error);
        tx.onabort=()=>reject(tx.error);
      });
      return true;
    },
    async getSettings(){
      const tx = db.transaction([STORE_SETTINGS],'readonly');
      const req = tx.objectStore(STORE_SETTINGS).get('app');
      const row = await new Promise((resolve)=>{
        req.onsuccess = ()=>resolve(req.result || { key:'app', baseCurrency:'TWD', monthlyBudgetTWD:0, savingsGoalTWD:0, nudges:true });
        req.onerror = ()=>resolve({ key:'app', baseCurrency:'TWD', monthlyBudgetTWD:0, savingsGoalTWD:0, nudges:true });
      });
      await new Promise((r)=>{tx.oncomplete=r});
      return row;
    },
    async setSettings(patch){
      const current = await this.getSettings();
      const next = { ...current, ...patch, key:'app' };
      const tx = db.transaction([STORE_SETTINGS],'readwrite');
      tx.objectStore(STORE_SETTINGS).put(next);
      await new Promise((resolve,reject)=>{
        tx.oncomplete=()=>resolve(true);
        tx.onerror=()=>reject(tx.error);
        tx.onabort=()=>reject(tx.error);
      });
      return next;
    },
    async updateCategoryModelFromNote(note, categoryId){
      if(!note || !categoryId) return false;
      const words = String(note).toLowerCase().split(/[^\p{L}\p{N}]+/u).filter(Boolean);
      if(words.length===0) return false;
      const tx = db.transaction([STORE_MODEL],'readwrite');
      const store = tx.objectStore(STORE_MODEL);
      await Promise.all(words.map(word=> new Promise((resolve)=>{
        const getReq = store.get(word);
        getReq.onsuccess = ()=>{
          const rec = getReq.result || { word, counts:{} };
          rec.counts[categoryId] = (rec.counts[categoryId]||0)+1;
          store.put(rec);
          resolve(true);
        };
        getReq.onerror = ()=>resolve(false);
      })));
      await new Promise((resolve)=>{ tx.oncomplete=()=>resolve(true); });
      return true;
    },
    async suggestCategoryFromNote(note){
      if(!note) return null;
      const words = String(note).toLowerCase().split(/[^\p{L}\p{N}]+/u).filter(Boolean);
      if(words.length===0) return null;
      const tx = db.transaction([STORE_MODEL],'readonly');
      const store = tx.objectStore(STORE_MODEL);
      const scores = {};
      await Promise.all(words.map(word=> new Promise((resolve)=>{
        const req = store.get(word);
        req.onsuccess = ()=>{
          const rec = req.result;
          if(rec && rec.counts){
            for(const [cat, cnt] of Object.entries(rec.counts)){
              scores[cat] = (scores[cat]||0) + Number(cnt||0);
            }
          }
          resolve(true);
        };
        req.onerror = ()=>resolve(false);
      })));
      await new Promise((r)=>{ tx.oncomplete=r });
      let best = null; let bestScore = 0;
      for(const [cat, sc] of Object.entries(scores)){
        if(sc>bestScore){ bestScore = sc; best = cat; }
      }
      return bestScore>0 ? best : null;
    },
    async getTransactions(){
      const tx = db.transaction([STORE_TX],'readonly');
      const store = tx.objectStore(STORE_TX);
      const req = store.getAll();
      const rows = await new Promise((resolve,reject)=>{
        req.onsuccess = ()=>resolve(req.result||[]);
        req.onerror = ()=>reject(req.error);
      });
      await new Promise((r)=>{tx.oncomplete=r});
      // keep newest first
      rows.sort((a,b)=> (b.date||'').localeCompare(a.date||''));
      return rows;
    },
    async addTransaction(txInput){
      const id = (crypto.randomUUID && crypto.randomUUID()) || String(Date.now())+Math.random().toString(16).slice(2);
      const record = { id, ...txInput };
      const tx = db.transaction([STORE_TX],'readwrite');
      tx.objectStore(STORE_TX).put(record);
      await new Promise((resolve,reject)=>{
        tx.oncomplete=()=>resolve(true);
        tx.onerror=()=>reject(tx.error);
        tx.onabort=()=>reject(tx.error);
      });
      // update model by note
      try{ await this.updateCategoryModelFromNote(record.note, record.categoryId); }catch(_){}
      return record;
    },
    async updateTransaction(id, patch){
      const current = await this.getTransactionById(id);
      if(!current) return null;
      const next = { ...current, ...patch };
      const tx = db.transaction([STORE_TX],'readwrite');
      tx.objectStore(STORE_TX).put(next);
      await new Promise((resolve,reject)=>{
        tx.oncomplete=()=>resolve(true);
        tx.onerror=()=>reject(tx.error);
        tx.onabort=()=>reject(tx.error);
      });
      return next;
    },
    async getTransactionById(id){
      const tx = db.transaction([STORE_TX],'readonly');
      const req = tx.objectStore(STORE_TX).get(id);
      const row = await new Promise((resolve,reject)=>{
        req.onsuccess = ()=>resolve(req.result||null);
        req.onerror = ()=>reject(req.error);
      });
      await new Promise((r)=>{tx.oncomplete=r});
      return row;
    },
    async deleteTransaction(id){
      const tx = db.transaction([STORE_TX],'readwrite');
      tx.objectStore(STORE_TX).delete(id);
      await new Promise((resolve,reject)=>{
        tx.oncomplete=()=>resolve(true);
        tx.onerror=()=>reject(tx.error);
        tx.onabort=()=>reject(tx.error);
      });
      return true;
    },
    async exportAll(){
      const [categories, transactions, settings, model] = await Promise.all([
        this.getCategories(),
        this.getTransactions(),
        this.getSettings(),
        (async()=>{
          const tx = db.transaction([STORE_MODEL],'readonly');
          const req = tx.objectStore(STORE_MODEL).getAll();
          const rows = await new Promise((resolve)=>{ req.onsuccess=()=>resolve(req.result||[]); req.onerror=()=>resolve([]); });
          await new Promise((r)=>{ tx.oncomplete=r });
          return rows;
        })()
      ]);
      return { categories, transactions, settings, model };
    },
    async importAll(data){
      if(!data || !Array.isArray(data.categories) || !Array.isArray(data.transactions)) return false;
      const tx = db.transaction([STORE_CATEGORIES, STORE_TX, STORE_SETTINGS, STORE_MODEL],'readwrite');
      const cat = tx.objectStore(STORE_CATEGORIES);
      const t = tx.objectStore(STORE_TX);
      const s = tx.objectStore(STORE_SETTINGS);
      const m = tx.objectStore(STORE_MODEL);
      // clear then bulk put
      cat.clear();
      t.clear();
      s.clear();
      m.clear();
      for(const c of data.categories){ cat.put(c); }
      for(const r of data.transactions){ t.put(r); }
      if(data.settings){ s.put({ key:'app', ...data.settings }); }
      if(Array.isArray(data.model)){
        for(const rec of data.model){ if(rec && rec.word){ m.put(rec); } }
      }
      await new Promise((resolve,reject)=>{
        tx.oncomplete=()=>resolve(true);
        tx.onerror=()=>reject(tx.error);
        tx.onabort=()=>reject(tx.error);
      });
      return true;
    }
  };

  const memory = {
    init(){ return delay(true); },
    getCategories(){ return delay([...inMemory.categories]); },
    async getSettings(){ return { key:'app', baseCurrency:'TWD', monthlyBudgetTWD:0, savingsGoalTWD:0, nudges:true }; },
    async setSettings(patch){ return { key:'app', baseCurrency:'TWD', monthlyBudgetTWD:0, savingsGoalTWD:0, nudges:true, ...patch }; },
    _model: {},
    async updateCategoryModelFromNote(note, categoryId){
      if(!note || !categoryId) return false;
      const words = String(note).toLowerCase().split(/[^\p{L}\p{N}]+/u).filter(Boolean);
      for(const w of words){
        const rec = this._model[w] || { word:w, counts:{} };
        rec.counts[categoryId] = (rec.counts[categoryId]||0)+1;
        this._model[w] = rec;
      }
      return true;
    },
    async suggestCategoryFromNote(note){
      const words = String(note||'').toLowerCase().split(/[^\p{L}\p{N}]+/u).filter(Boolean);
      const scores = {};
      for(const w of words){
        const rec = this._model[w];
        if(rec){ for(const [cat,cnt] of Object.entries(rec.counts)){ scores[cat]=(scores[cat]||0)+Number(cnt||0); } }
      }
      let best=null, bestScore=0;
      for(const [cat,sc] of Object.entries(scores)){ if(sc>bestScore){best=cat;bestScore=sc;} }
      return bestScore>0?best:null;
    },
    addCategory(name){
      const id = name.trim().toLowerCase().replace(/\s+/g,'-');
      if(!id) return delay(null);
      if(inMemory.categories.some(c=>c.id===id)) return delay(null);
      const cat = { id, name: name.trim() };
      inMemory.categories.push(cat);
      return delay(cat);
    },
    deleteCategory(id){
      const used = inMemory.transactions.some(t=>t.categoryId===id);
      if(used) return delay(false);
      const before = inMemory.categories.length;
      inMemory.categories = inMemory.categories.filter(c=>c.id!==id);
      return delay(inMemory.categories.length < before);
    },
    getTransactions(){ return delay([...inMemory.transactions]); },
    addTransaction(tx){
      const id = crypto.randomUUID ? crypto.randomUUID() : String(Date.now())+Math.random().toString(16).slice(2);
      const record = { id, ...tx };
      inMemory.transactions.unshift(record);
      return delay(record);
    },
    updateTransaction(id, patch){
      const idx = inMemory.transactions.findIndex(t=>t.id===id);
      if(idx<0) return delay(null);
      inMemory.transactions[idx] = { ...inMemory.transactions[idx], ...patch };
      return delay(inMemory.transactions[idx]);
    },
    deleteTransaction(id){
      const before = inMemory.transactions.length;
      inMemory.transactions = inMemory.transactions.filter(t=>t.id!==id);
      return delay(inMemory.transactions.length < before);
    },
    exportAll(){ return delay({ categories: inMemory.categories, transactions: inMemory.transactions, settings: { key:'app', baseCurrency:'TWD', monthlyBudgetTWD:0, savingsGoalTWD:0, nudges:true }, model: Object.values(this._model) }); },
    importAll(data){
      if(!data || !Array.isArray(data.categories) || !Array.isArray(data.transactions)) return delay(false);
      inMemory.categories = [...data.categories];
      inMemory.transactions = [...data.transactions];
      return delay(true);
    }
  };

  const remote = {
    base(){
      // 優先使用同源（避免在雲端環境誤用過去本機的 localhost 設定）
      try{
        const origin = location && location.origin ? String(location.origin) : '';
        if(origin && /^https?:\/\//.test(origin) && !/localhost|127\.0\.0\.1/.test(origin)){
          return origin.replace(/\/$/,'');
        }
      }catch(_){ }
      // 回退：使用使用者設定（例如本機開發）
      try{
        const saved = (localStorage.getItem('serverUrl')||'').trim();
        if(saved) return saved.replace(/\/$/,'');
      }catch(_){ }
      // 最後回退：若無同源、無設定，嘗試使用 location.origin 或空字串
      try{ return location.origin; }catch(_){ return ''; }
    },
    async init(){ return true; },
    async getCategories(){ const r = await fetch(`${this.base()}/api/categories`); return await r.json(); },
    async addCategory(name){ const r = await fetch(`${this.base()}/api/categories`,{ method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ name }) }); const j=await r.json(); return j.category||null; },
    async deleteCategory(id){ const r = await fetch(`${this.base()}/api/categories/${encodeURIComponent(id)}`,{ method:'DELETE' }); const j=await r.json(); return !!j.ok; },
    async getSettings(){ const r = await fetch(`${this.base()}/api/settings`); return await r.json(); },
    async setSettings(patch){ const r = await fetch(`${this.base()}/api/settings`,{ method:'PUT', headers:{'Content-Type':'application/json'}, body: JSON.stringify(patch) }); return await r.json(); },
    async suggestCategoryFromNote(note){ const r = await fetch(`${this.base()}/api/model/suggest`,{ method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ note }) }); const j=await r.json(); return j.categoryId||null; },
    async updateCategoryModelFromNote(note, categoryId){ await fetch(`${this.base()}/api/model/update`,{ method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ note, categoryId }) }); return true; },
    async getTransactions(){ const r = await fetch(`${this.base()}/api/transactions`); const rows=await r.json(); return rows.sort((a,b)=> (b.date||'').localeCompare(a.date||'')); },
    async addTransaction(payload){ const r = await fetch(`${this.base()}/api/transactions`,{ method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) }); const j=await r.json(); return j.transaction; },
    async updateTransaction(id, patch){ const r = await fetch(`${this.base()}/api/transactions/${encodeURIComponent(id)}`,{ method:'PUT', headers:{'Content-Type':'application/json'}, body: JSON.stringify(patch) }); const j=await r.json(); return j.transaction; },
    async getTransactionById(id){ const r = await fetch(`${this.base()}/api/transactions/${encodeURIComponent(id)}`); const j=await r.json(); return j.transaction||null; },
    async deleteTransaction(id){ const r = await fetch(`${this.base()}/api/transactions/${encodeURIComponent(id)}`,{ method:'DELETE' }); const j=await r.json(); return !!j.ok; },
    async exportAll(){ const [categories, transactions, settings] = await Promise.all([ this.getCategories(), this.getTransactions(), this.getSettings() ]); return { categories, transactions, settings, model: [] }; },
    async importAll(data){
      await fetch(`${this.base()}/api/sync/import`,{ method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(data) });
      return true;
    }
  };

  function choose(){
    try{
      const url = (localStorage.getItem('serverUrl')||'').trim();
      if(url){ return remote; }
    }catch(_){ }
    return supportsIndexedDB ? idb : memory;
  }
  window.DB = choose();
})();

