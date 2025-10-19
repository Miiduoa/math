(function(){
  const $ = (sel, root=document) => root.querySelector(sel);
  const $$ = (sel, root=document) => Array.from(root.querySelectorAll(sel));
  const ADMIN_LINE_USER_ID = 'U5c7738d89a59ff402fd6b56f5472d351';

  // Resolve API base URL for AI endpoints（優先使用設定的伺服器 URL，其次才用同源）
  function apiBase(preferred){
    try{
      const cand = String(preferred||'').trim();
      if(/^https?:\/\//.test(cand)) return cand.replace(/\/$/,'');
    }catch(_){ }
    try{
      const saved = (localStorage.getItem('serverUrl')||'').trim();
      if(/^https?:\/\//.test(saved)) return saved.replace(/\/$/,'');
    }catch(_){ }
    try{
      const origin = (typeof location!=='undefined' && /^https?:/.test(location.origin)) ? location.origin : '';
      if(origin) return origin.replace(/\/$/,'');
    }catch(_){ }
    // Sensible default for local dev
    return 'http://localhost:8787';
  }

  function showDialogSafe(dialog){
    if(!dialog) return;
    try{ dialog.showModal?.(); return; }catch(_){ }
    try{ dialog.show?.(); return; }catch(_){ }
    if(!dialog.open) dialog.setAttribute('open','');
  }

  function formatAmount(n){
    const sign = n < 0 ? '-' : '';
    const v = Math.abs(Number(n)||0);
    return sign + v.toLocaleString(undefined,{minimumFractionDigits:2, maximumFractionDigits:2});
  }
  function toBaseCurrency(amount, currency, rate){
    const n = Number(amount)||0;
    const r = Number(rate)||1;
    if((currency||'TWD')==='TWD') return n;
    return n * r;
  }

  function today(){
    const d = new Date();
    const m = String(d.getMonth()+1).padStart(2,'0');
    const day = String(d.getDate()).padStart(2,'0');
    return `${d.getFullYear()}-${m}-${day}`;
  }

  function renderCategories(categories){
    const select = $('#txCategory');
    select.innerHTML = categories.map(c=>`<option value="${c.id}">${c.name}</option>`).join('');
    const list = $('#categoryList');
    list.innerHTML = categories.map(c=>`<li class="category-item">
      <span>${c.name}</span>
      <button data-id="${c.id}" class="danger">刪除</button>
    </li>`).join('');
  }

  function renderTransactions(items){
    const ul = $('#txList');
    if(items.length===0){
      ul.innerHTML = '<li class="tx-item"><span>目前沒有交易紀錄</span></li>';
      updateSummary([]);
      return;
    }
    ul.innerHTML = items.map(t=>{
      const amountClass = t.type === 'income' ? 'income' : 'expense';
      const sign = t.type === 'income' ? '' : '-';
      const base = toBaseCurrency(t.amount, t.currency||'TWD', t.rate||1);
      const amountText = `${t.currency||'TWD'} ${formatAmount(t.amount)}`;
      const claimText = (Number(t.claimAmount)||0) > 0 ? `<br><small>請款 ${t.currency||'TWD'} ${formatAmount(t.claimAmount)}</small>` : '';
      const emo = t.emotion ? `<div class="emotion">情緒：${t.emotion}${t.motivation?`｜動機：${t.motivation}`:''}</div>` : (t.motivation?`<div class="emotion">動機：${t.motivation}</div>`:'');
      const claimBadge = (t.type==='expense')
        ? (t.claimed ? '<span class="badge success">已請款</span>' : '<span class="badge pending">未請款</span>')
        : '';
      return `<li class="tx-item" data-id="${t.id}">
        <div>
          <div>${t.note||'(無備註)'}・<small>${t.categoryName||t.categoryId}</small> ${claimBadge}</div>${emo}
          <small>${t.date}</small>
        </div>
        <div class="tx-amount ${amountClass}">${sign}$${formatAmount(base)}<br><small>${amountText}</small>${claimText}</div>
        <div class="tx-actions">
          <button class="ghost" data-action="toggle-claim">${t.claimed ? '標記未請款' : '標記已請款'}</button>
          <button class="ghost" data-action="repeat">重複</button>
          <button class="ghost" data-action="edit">編輯</button>
          <button class="ghost danger" data-action="delete">刪除</button>
        </div>
      </li>`;
    }).join('');
    updateSummary(items);
  }

  function updateSummary(items){
    const income = items
      .filter(t=>t.type==='income')
      .reduce((s,t)=>s+toBaseCurrency(t.amount, t.currency||'TWD', t.rate||1),0);
    const expense = items
      .filter(t=>t.type==='expense')
      .reduce((s,t)=>s+toBaseCurrency(t.amount, t.currency||'TWD', t.rate||1),0);
    $('#summaryIncome').textContent = `$${formatAmount(income)}`;
    $('#summaryExpense').textContent = `$${formatAmount(expense)}`;
    $('#summaryBalance').textContent = `$${formatAmount(income-expense)}`;
  }

  async function refresh(){
    const [categories, txs, latestNotice] = await Promise.all([
      DB.getCategories(),
      DB.getTransactions(),
      fetch('/api/notice/latest').then(r=> r.ok ? r.json() : { ok:false }).catch(()=>({ ok:false }))
    ]);
    const categoryMap = new Map(categories.map(c=>[c.id, c.name]));
    renderCategories(categories);
    const enriched = txs.map(t=>({ ...t, categoryName: categoryMap.get(t.categoryId)||t.categoryId }));
    renderTransactions(enriched);
    // render broadcast notice if any
    try{
      const wrap = document.getElementById('broadcastNotice');
      if(wrap){
        const msg = latestNotice?.latest?.message||'';
        if(msg){
          wrap.querySelector('.notice__content').innerHTML = `<strong>公告：</strong>${msg}`;
          wrap.style.display = 'flex';
          wrap.querySelector('.notice__actions .ghost')?.addEventListener('click', ()=>{ wrap.style.display='none'; });
        }else{
          wrap.style.display = 'none';
        }
      }
    }catch(_){ }
    if($('#statsMode')){ renderChart(); }
    if($('#calendarGrid')){ renderCalendar(enriched); }
  }

  function applyAppearance(appearance){
    const root = document.documentElement;
    root.classList.remove('theme-light','theme-dark','theme-oled');
    let themeColor = '#0ea5e9';
    if(appearance==='light'){
      root.classList.add('theme-light');
      themeColor = '#0ea5e9';
    }else if(appearance==='dark-oled'){
      root.classList.add('theme-oled','theme-dark');
      themeColor = '#000000';
    }else{
      // system
      const isDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
      if(isDark){
        // Use OLED when system is dark
        root.classList.add('theme-oled','theme-dark');
        themeColor = '#000000';
      }else{
        root.classList.remove('theme-light','theme-dark','theme-oled');
        themeColor = '#0ea5e9';
      }
    }
    // update theme-color meta
    const lightMeta = document.querySelector('meta[name="theme-color"][media*="light"]');
    const darkMeta = document.querySelector('meta[name="theme-color"][media*="dark"]');
    if(appearance==='light'){
      if(lightMeta) lightMeta.setAttribute('content','#0ea5e9');
      if(darkMeta) darkMeta.setAttribute('content','#000000');
    }else if(appearance==='dark-oled'){
      if(lightMeta) lightMeta.setAttribute('content','#0ea5e9');
      if(darkMeta) darkMeta.setAttribute('content','#000000');
    }else{
      if(lightMeta) lightMeta.setAttribute('content','#0ea5e9');
      if(darkMeta) darkMeta.setAttribute('content','#000000');
    }
    // for iOS PWA header coloring (best-effort)
    const metaAny = document.querySelector('meta[name="theme-color"]:not([media])');
    if(metaAny) metaAny.setAttribute('content', themeColor);
  }

  function filterAndRender(){
    const q = $('#searchInput').value.trim().toLowerCase();
    const type = $('#filterType').value;
    const onlyUnclaimed = $('#filterUnclaimed')?.checked;
    const sortUnclaimedFirst = $('#sortUnclaimedFirst')?.checked;
    DB.getTransactions().then(items=>{
      let filtered = items;
      if(type!=='all') filtered = filtered.filter(t=>t.type===type);
      if(dateFilter){ filtered = filtered.filter(t=> (t.date||'') === dateFilter); }
      if(q){
        filtered = filtered.filter(t=>
          (t.note||'').toLowerCase().includes(q) || (t.categoryName||t.categoryId||'').toLowerCase().includes(q)
        );
      }
      if(onlyUnclaimed){
        // 視為：支出且未請款（claimed 僅在明確為 true 才視為已請款）
        filtered = filtered.filter(t=> t.type==='expense' && t.claimed !== true);
      }
      if(sortUnclaimedFirst){
        filtered = [...filtered].sort((a,b)=>{
          const au = (a.type==='expense' && a.claimed !== true) ? 1 : 0;
          const bu = (b.type==='expense' && b.claimed !== true) ? 1 : 0;
          if(bu!==au) return bu-au; // unclaimed first
          return (b.date||'').localeCompare(a.date||'');
        });
      }
      renderTransactions(filtered);
    });
  }

  function bindEvents(){
    // AI Responses panel bindings
    async function callResponsesApi(path, body){
      try{
        const base = apiBase();
        const resp = await fetch(`${base}${path}`, { method:'POST', headers:{ 'Content-Type':'application/json' }, body: JSON.stringify(body||{}) });
        const j = await resp.json().catch(()=>({ ok:false }));
        if(!resp.ok || !j.ok){ throw new Error('upstream'); }
        return String(j.output||'');
      }catch(err){ return '呼叫失敗，請稍後再試'; }
    }
    const respOut = $('#respOutput');
    $('#respTextBtn')?.addEventListener('click', async ()=>{
      const text = $('#respInput')?.value||'';
      respOut.textContent = '思考中…';
      respOut.textContent = await callResponsesApi('/api/ai/responses/text', { input: text });
    });
    $('#respVisionBtn')?.addEventListener('click', async ()=>{
      const text = $('#respInput')?.value||'';
      const imageUrl = $('#respImageUrl')?.value||'';
      respOut.textContent = '思考中…';
      respOut.textContent = await callResponsesApi('/api/ai/responses/vision', { text, imageUrl });
    });
    $('#respWebBtn')?.addEventListener('click', async ()=>{
      const text = $('#respInput')?.value||'';
      respOut.textContent = '思考中…';
      respOut.textContent = await callResponsesApi('/api/ai/responses/web_search', { input: text });
    });
    $('#respFileBtn')?.addEventListener('click', async ()=>{
      const text = $('#respInput')?.value||'';
      respOut.textContent = '思考中…';
      respOut.textContent = await callResponsesApi('/api/ai/responses/file_search', { input: text });
    });
    $('#respStreamBtn')?.addEventListener('click', async ()=>{
      const text = $('#respInput')?.value||'';
      respOut.textContent = '思考中…';
      respOut.textContent = await callResponsesApi('/api/ai/responses/stream', { input: text });
    });
    $('#respAgentsBtn')?.addEventListener('click', async ()=>{
      const text = $('#respInput')?.value||'';
      respOut.textContent = '思考中…';
      respOut.textContent = await callResponsesApi('/api/ai/agents/triage', { input: text });
    });
    // 手動同步：將本機 IndexedDB 資料上傳到伺服器
    document.getElementById('syncBtn')?.addEventListener('click', async ()=>{
      try{
        const hasIdb = typeof window!=='undefined' && window.DB_idb;
        const hasRemote = typeof window!=='undefined' && window.DB_remote;
        if(!hasIdb || !hasRemote){ alert('無本機或伺服器資料來源可同步'); return; }
        // 匯出本機資料
        await window.DB_idb.init?.();
        const localDump = await window.DB_idb.exportAll();
        // 切換到伺服器後匯入
        const prev = window.DB;
        window.DB = window.DB_remote;
        await DB.init?.();
        await DB.importAll(localDump);
        await refresh();
        // 切回原本 provider（僅在使用者尚未登入前想維持本機使用體驗）
        window.DB = prev;
        alert('同步完成');
      }catch(err){
        alert('同步失敗，請稍後再試');
      }
    });
    $('#txForm').addEventListener('submit', async (e)=>{
      e.preventDefault();
      const form = e.currentTarget;
      const editingId = form.dataset.editingId || null;
      const payload = {
        date: $('#txDate').value,
        type: $('#txType').value,
        categoryId: $('#txCategory').value,
        currency: $('#txCurrency') ? $('#txCurrency').value : 'TWD',
        rate: Number($('#txRate') ? $('#txRate').value : 1) || 1,
        amount: Number($('#txAmount').value),
        claimAmount: Number($('#txClaimAmount') ? $('#txClaimAmount').value : 0) || 0,
        claimed: $('#txClaimed') ? $('#txClaimed').checked : false,
        emotion: $('#txEmotion') ? $('#txEmotion').value : '',
        motivation: $('#txMotivation') ? $('#txMotivation').value.trim() : '',
        note: $('#txNote').value.trim()
      };
      if(!payload.date || !payload.categoryId || !payload.type || !Number.isFinite(payload.amount)){
        alert('請完整填寫欄位');
        return;
      }
      if(editingId){
        await DB.updateTransaction(editingId, payload);
      }else{
        await DB.addTransaction(payload);
      }
      clearEditingState();
      form.reset();
      $('#txDate').value = today();
      refresh();
    });

    $('#searchInput').addEventListener('input', filterAndRender);
    $('#filterType').addEventListener('change', filterAndRender);
    $('#filterUnclaimed')?.addEventListener('change', filterAndRender);
    $('#sortUnclaimedFirst')?.addEventListener('change', filterAndRender);

    $('#manageCategoriesBtn').addEventListener('click', ()=>{
      $('#categoryDialog').showModal();
    });
    $('#addCategoryBtn').addEventListener('click', async ()=>{
      const name = $('#newCategoryInput').value.trim();
      if(!name) return;
      await DB.addCategory(name);
      $('#newCategoryInput').value='';
      refresh();
    });
    $('#categoryList').addEventListener('click', async (e)=>{
      const btn = e.target.closest('button');
      if(!btn) return;
      const id = btn.dataset.id;
      const ok = await DB.deleteCategory(id);
      if(!ok){
        alert('此分類已有交易使用，無法刪除');
      }
      refresh();
    });

    $('#txList').addEventListener('click', async (e)=>{
      const btn = e.target.closest('button');
      if(!btn) return;
      const li = e.target.closest('li[data-id]');
      const id = li?.dataset.id;
      if(btn.dataset.action==='toggle-claim' && id){
        const tx = await DB.getTransactionById(id);
        if(!tx) return;
        await DB.updateTransaction(id, { claimed: tx.claimed ? false : true });
        refresh();
        return;
      }
      if(btn.dataset.action==='repeat' && id){
        const tx = await DB.getTransactionById(id);
        if(!tx) return;
        const payload = {
          date: today(),
          type: tx.type,
          categoryId: tx.categoryId,
          currency: tx.currency||'TWD',
          rate: tx.rate||1,
          amount: tx.amount,
          claimAmount: 0,
          claimed: false,
          emotion: tx.emotion||'',
          motivation: tx.motivation||'',
          note: tx.note||''
        };
        await DB.addTransaction(payload);
        refresh();
        return;
      }
      if(btn.dataset.action==='delete' && id){
        if(confirm('確定刪除此筆交易？')){
          await DB.deleteTransaction(id);
          refresh();
        }
      }
      if(btn.dataset.action==='edit' && id){
        const tx = await DB.getTransactionById(id);
        if(!tx) return;
        $('#txDate').value = tx.date;
        $('#txType').value = tx.type;
        $('#txCategory').value = tx.categoryId;
        if($('#txCurrency')) $('#txCurrency').value = tx.currency||'TWD';
        if($('#txRate')) $('#txRate').value = tx.rate||1;
        $('#txAmount').value = tx.amount;
        if($('#txClaimAmount')) $('#txClaimAmount').value = tx.claimAmount||0;
        if($('#txClaimed')) $('#txClaimed').checked = !!tx.claimed;
        if($('#txEmotion')) $('#txEmotion').value = tx.emotion||'';
        if($('#txMotivation')) $('#txMotivation').value = tx.motivation||'';
        $('#txNote').value = tx.note||'';
        const form = $('#txForm');
        form.dataset.editingId = id;
        $('#submitBtn').textContent = '儲存變更';
        $('#cancelEditBtn').style.display = '';
        window.scrollTo({ top: 0, behavior: 'smooth' });
      }
    });

    $('#exportJsonBtn').addEventListener('click', async ()=>{
      const data = await DB.exportAll();
      const blob = new Blob([JSON.stringify(data,null,2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `ledger-export-${Date.now()}.json`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    });
    $('#exportHtmlBtn')?.addEventListener('click', async ()=>{
      const [settings, txs, cats] = await Promise.all([ DB.getSettings?.(), DB.getTransactions(), DB.getCategories() ]);
      const now = new Date();
      const ym = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
      const monthTx = txs.filter(t=> (t.date||'').startsWith(ym));
      const toBase = (t)=> toBaseCurrency(t.amount, t.currency||'TWD', t.rate||1);
      const income = monthTx.filter(t=>t.type==='income').reduce((s,t)=> s+toBase(t),0);
      const expense = monthTx.filter(t=>t.type==='expense').reduce((s,t)=> s+toBase(t),0);
      const net = income-expense;
      const catName = (id)=> cats.find(c=>c.id===id)?.name||id;
      const byCat = new Map();
      for(const t of monthTx){ if(t.type!=='expense') continue; const v=toBase(t); byCat.set(t.categoryId,(byCat.get(t.categoryId)||0)+v); }
      const catRows = Array.from(byCat.entries()).sort((a,b)=>b[1]-a[1]).map(([id,v])=>`<tr><td>${catName(id)}</td><td style="text-align:right">$${formatAmount(v)}</td></tr>`).join('');
      const recentRows = monthTx.slice(0,30).map(t=>`<tr><td>${t.date}</td><td>${catName(t.categoryId)}</td><td>${(t.note||'').replace(/[<>&]/g,'')}</td><td style="text-align:right">${t.type==='income'?'':'-'}$${formatAmount(toBase(t))}</td></tr>`).join('');
      // Build simple monthly chart for current year
      const y = String(now.getFullYear());
      const months = Array.from({length:12}, (_,i)=> `${y}-${String(i+1).padStart(2,'0')}`);
      const monthData = months.map(m=>{
        const group = txs.filter(t=> (t.date||'').startsWith(m));
        const inc = group.filter(t=>t.type==='income').reduce((s,t)=> s+toBase(t),0);
        const exp = group.filter(t=>t.type==='expense').reduce((s,t)=> s+toBase(t),0);
        return { label:m.slice(5), value: Math.max(inc-exp, 0) };
      });
      const maxV = Math.max(1, ...monthData.map(d=>d.value));
      const bars = monthData.map(d=>{
        const h = Math.round((d.value / maxV) * 160) + 8;
        return `<div class="bar" style="height:${h}px" title="${d.label}: $${formatAmount(d.value)}"></div>`;
      }).join('');
      // Try AI analysis
      let aiText = '';
      try{
        const base = apiBase();
        if(base){
          const resp = await fetch(`${base}/api/ai`,{
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({
              messages:[{ role:'user', content: '請以繁體中文輸出一段針對本月記帳資料的分析，包含：1) 消費概況亮點與異常 2) 前三大分類與建議 3) 下月行動建議（100~180字）。' }],
              context: { settings, transactions: txs, categories: cats },
              mode:'chat'
            })
          });
          const j = await resp.json().catch(()=>({}));
          aiText = (j && j.reply) ? String(j.reply) : '';
        }
      }catch(_){ aiText=''; }
      if(!aiText){
        // Fallback local analysis
        const top = Array.from(byCat.entries()).sort((a,b)=>b[1]-a[1]).slice(0,3).map(([id,v])=> `${catName(id)}$${formatAmount(v)}`).join('、');
        aiText = `離線分析：本月結餘 $${formatAmount(net)}。前幾大支出：${top||'無' }。建議設定分類預算與提醒，關注波動較大的項目。`;
      }
      const html = `<!doctype html><html><head><meta charset="utf-8"><title>iOS 風格記帳報告</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
          :root{ --text:#0f172a; --muted:#475569; --primary:#0ea5e9; --border:rgba(2,6,23,0.08); --glass:rgba(255,255,255,0.7); }
          @media (prefers-color-scheme: dark){ :root{ --text:#e2e8f0; --muted:#94a3b8; --primary:#38bdf8; --border:rgba(148,163,184,0.18); --glass:rgba(15,23,42,0.6) } }
          body{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial;background:
            radial-gradient(1200px 800px at 15% 10%, rgba(14,165,233,0.14), rgba(14,165,233,0) 55%),
            radial-gradient(1000px 700px at 85% 90%, rgba(56,189,248,0.16), rgba(56,189,248,0) 55%),
            linear-gradient(180deg,#f8fbff,#eef3f8);
            color:var(--text);margin:0;padding:24px }
          .wrap{max-width:960px;margin:0 auto}
          .card{background:var(--glass);backdrop-filter:blur(18px) saturate(170%);-webkit-backdrop-filter:blur(18px) saturate(170%);
            border:1px solid var(--border);border-radius:18px;box-shadow:0 10px 30px rgba(2,6,23,0.12);padding:16px;margin:12px 0;position:relative}
          .card::after{content:"";position:absolute;inset:0;border-radius:inherit;pointer-events:none;box-shadow:inset 0 1px 0 rgba(255,255,255,0.3)}
          h1,h2{margin:0 0 12px}
          .pill{display:inline-block;padding:4px 10px;border-radius:999px;border:1px solid var(--border);background:rgba(255,255,255,0.6)}
          .summary{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}
          table{width:100%;border-collapse:collapse}
          th,td{padding:8px;border-bottom:1px solid var(--border)}
          th{color:var(--muted);text-align:left;font-weight:600}
          .chart{height:180px;display:grid;align-items:end;gap:6px;grid-auto-flow:column;border-top:1px dashed var(--border);padding-top:12px}
          .bar{background:linear-gradient(180deg,#22d3ee,#0ea5e9);border-radius:6px;min-width:12px}
          .muted{color:var(--muted)}
          .ai{white-space:pre-wrap;line-height:1.6}
        </style></head><body><div class="wrap">
        <div class="card"><h1>記帳報告</h1><div class="pill">${ym}</div><div class="muted" style="margin-top:6px">自動產生 • iOS 玻璃風格</div></div>
        <div class="card"><h2>摘要</h2><div class="summary">
          <div><div class="pill">收入 (TWD)</div><div style="font-size:22px;font-weight:700;margin-top:6px">$${formatAmount(income)}</div></div>
          <div><div class="pill">支出 (TWD)</div><div style="font-size:22px;font-weight:700;margin-top:6px">$${formatAmount(expense)}</div></div>
          <div><div class="pill">結餘 (TWD)</div><div style="font-size:22px;font-weight:700;margin-top:6px">$${formatAmount(net)}</div></div>
        </div></div>
        <div class="card"><h2>月度走勢（淨額）</h2><div class="chart">${bars}</div></div>
        <div class="card"><h2>AI 分析</h2><div class="ai">${(aiText||'').replace(/</g,'&lt;')}</div></div>
        <div class="card"><h2>分類支出</h2><table><thead><tr><th>分類</th><th style="text-align:right">金額</th></tr></thead><tbody>${catRows||'<tr><td colspan="2">本月尚無支出</td></tr>'}</tbody></table></div>
        <div class="card"><h2>最近交易</h2><table><thead><tr><th>日期</th><th>分類</th><th>備註</th><th style="text-align:right">金額 (TWD)</th></tr></thead><tbody>${recentRows||'<tr><td colspan="4">無資料</td></tr>'}</tbody></table></div>
      </div></body></html>`;
      const blob = new Blob([html], { type:'text/html' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url; a.download = `ledger-report-${ym}.html`;
      document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url);
    });
    $('#importJsonInput').addEventListener('change', async (e)=>{
      const file = e.target.files?.[0];
      if(!file) return;
      const text = await file.text();
      let parsed=null;
      try{ parsed = JSON.parse(text); }
      catch(_){ alert('匯入失敗，檔案非有效 JSON'); e.target.value=''; return; }
      if(!parsed || !Array.isArray(parsed.categories) || !Array.isArray(parsed.transactions)){
        alert('匯入失敗，格式不正確（缺少 categories 或 transactions）'); e.target.value=''; return;
      }
      // render preview
      const dialog = document.getElementById('importPreviewDialog');
      const form = document.getElementById('importPreviewForm');
      const body = document.getElementById('importPreviewBody');
      const cats = Array.isArray(parsed.categories) ? parsed.categories.length : 0;
      const txs = Array.isArray(parsed.transactions) ? parsed.transactions.length : 0;
      const settings = parsed.settings ? Object.keys(parsed.settings).length : 0;
      const model = Array.isArray(parsed.model) ? parsed.model.length : 0;
      body.innerHTML = `<div class="summary">
        <div><span class="label">分類</span><span>${cats} 筆</span></div>
        <div><span class="label">交易</span><span>${txs} 筆</span></div>
        <div><span class="label">設定</span><span>${settings} 欄位</span></div>
        <div><span class="label">模型</span><span>${model} 詞</span></div>
      </div>
      <div style="margin-top:8px"><small>範例交易：</small><pre style="white-space:pre-wrap;background:transparent">${
        JSON.stringify(parsed.transactions.slice(0,3),null,2)
      }</pre></div>`;
      function showDialogSafe(d){ try{ d.showModal?.(); return; }catch(_){ } try{ d.show?.(); return; }catch(_){ } if(!d.open) d.setAttribute('open',''); }
      showDialogSafe(dialog);
      form.onsubmit = async (ev)=>{
        const submitter = ev.submitter;
        if(submitter && submitter.value==='cancel'){ return; }
        ev.preventDefault();
        try{
          const ok = await DB.importAll(parsed);
          if(!ok){ alert('匯入失敗，格式不正確'); return; }
          dialog.close();
          await refresh();
          alert('匯入完成');
        }catch(_){ alert('匯入失敗'); }
      };
      e.target.value='';
    });

    // Backup notice
    const notice = document.getElementById('backupNotice');
    const dismissBtn = document.getElementById('backupDismissBtn');
    const exportBtn = document.getElementById('backupExportBtn');
    function shouldShowNotice(){
      try{
        const last = Number(localStorage.getItem('backupNoticeAt')||0);
        return Date.now() - last > 1000*60*60*24*14; // 14 days
      }catch(_){ return true; }
    }
    function markNoticeSeen(){ try{ localStorage.setItem('backupNoticeAt', String(Date.now())); }catch(_){ } }
    if(notice && shouldShowNotice()) notice.style.display='flex';
    dismissBtn?.addEventListener('click', ()=>{ notice.style.display='none'; markNoticeSeen(); });
    exportBtn?.addEventListener('click', ()=>{ document.getElementById('exportJsonBtn')?.click(); markNoticeSeen(); });
    // Admin broadcast UI
    (async ()=>{
      try{
        const info = await fetch('/api/admin/info').then(r=> r.ok ? r.json() : { ok:false }).catch(()=>({ ok:false }));
        let isAdmin = Boolean(info && info.ok && info.admin);
        if(!isAdmin){
          try{
            const me = await fetch('/api/me').then(r=> r.ok ? r.json() : { ok:false }).catch(()=>({ ok:false }));
            const uid = me && me.ok && me.user && me.user.id ? String(me.user.id) : '';
            if(uid.startsWith('line:') && uid.slice('line:'.length) === ADMIN_LINE_USER_ID){
              isAdmin = true;
            }
          }catch(_){ /* ignore */ }
        }
        const wrap = document.getElementById('adminBroadcast');
        if(isAdmin && wrap){
          wrap.style.display = '';
          const input = document.getElementById('adminBroadcastInput');
          const btn = document.getElementById('adminBroadcastBtn');
          btn?.addEventListener('click', async ()=>{
            const text = (input?.value||'').trim();
            if(!text){ alert('請輸入公告內容'); return; }
            try{
              const res = await fetch('/api/admin/broadcast', { method:'POST', headers:{ 'Content-Type':'application/json' }, body: JSON.stringify({ message: text }) });
              if(!res.ok){ throw new Error('broadcast failed'); }
              input.value = '';
              alert('已發送公告');
              // refresh to display latest notice
              setTimeout(()=>{ refresh(); }, 200);
            }catch(err){ alert('發送失敗'); }
          });
        }
        // Model admin
        const modelWrap = document.getElementById('adminModel');
        if(isAdmin && modelWrap){
          modelWrap.style.display = '';
          const backfillBtn = document.getElementById('modelBackfillBtn');
          const inspectBtn = document.getElementById('modelInspectBtn');
          const view = document.getElementById('modelInspectView');
          backfillBtn?.addEventListener('click', async ()=>{
            try{
              const res = await fetch('/api/model/backfill',{ method:'POST' });
              if(!res.ok) throw new Error('backfill');
              alert('回填完成');
            }catch(_){ alert('回填失敗'); }
          });
          inspectBtn?.addEventListener('click', async ()=>{
            try{
              const res = await fetch('/api/model/inspect');
              if(!res.ok) throw new Error('inspect');
              const j = await res.json();
              const rows = Array.isArray(j.model) ? j.model : [];
              const html = rows.slice(0,200).map(r=>`<div style="padding:4px 0; border-bottom:1px dashed var(--glass-border)"><strong>${(r.word||'').replace(/[<>&]/g,'')}</strong><br><small>${Object.entries(r.counts||{}).map(([k,v])=>`${k}:${v}`).join(' · ')}</small></div>`).join('') || '<div>目前沒有模型資料</div>';
              view.style.display='block';
              view.innerHTML = html;
            }catch(_){ view.style.display='block'; view.innerHTML='<div>讀取失敗</div>'; }
          });
        }
      }catch(_){ /* not admin */ }
    })();

    $('#cancelEditBtn').addEventListener('click', ()=>{
      clearEditingState();
    });

    // Toggle advanced fields
    const advBtn = $('#toggleAdvancedBtn');
    advBtn?.addEventListener('click', ()=>{
      document.body.classList.toggle('advanced-on');
      advBtn.textContent = document.body.classList.contains('advanced-on') ? '隱藏選項' : '更多選項';
    });

    // Quick Add FAB & dialog
    const fab = $('#fabQuickAdd');
    const quickDialog = $('#quickAddDialog');
    const quickForm = $('#quickAddForm');
    const quickAmount = $('#quickAmountInput');
    const quickNote = $('#quickNoteInput');
    const quickChips = $('#quickCategoryChips');
    const quickAmountChips = $('#quickAmountChips');
    const quickTypeExpenseBtn = $('#quickTypeExpenseBtn');
    const quickTypeIncomeBtn = $('#quickTypeIncomeBtn');
    const quickKeepOpenToggle = $('#quickKeepOpenToggle');
    const quickNoteChips = $('#quickNoteChips');
    let quickType = 'expense';

    function setQuickType(t){
      quickType = t;
      if(quickType==='expense'){
        quickTypeExpenseBtn.classList.add('active');
        quickTypeIncomeBtn.classList.remove('active');
      }else{
        quickTypeIncomeBtn.classList.add('active');
        quickTypeExpenseBtn.classList.remove('active');
      }
    }
    quickTypeExpenseBtn?.addEventListener('click', ()=> setQuickType('expense'));
    quickTypeIncomeBtn?.addEventListener('click', ()=> setQuickType('income'));

    fab?.addEventListener('click', async ()=>{
      // render frequent categories as chips (top 8 by recent usage)
      const txs = await DB.getTransactions();
      const counts = new Map();
      for(const t of txs){ counts.set(t.categoryId, (counts.get(t.categoryId)||0)+1); }
      const cats = await DB.getCategories();
      const sorted = cats.sort((a,b)=> (counts.get(b.id)||0) - (counts.get(a.id)||0)).slice(0,8);
      quickChips.innerHTML = sorted.map(c=>`<span class="chip" data-cat="${c.id}">${c.name}</span>`).join('');
      let selectedCat = sorted[0]?.id || cats[0]?.id || '';
      function markActive(){
        quickChips.querySelectorAll('.chip').forEach(el=>{
          if(el.dataset.cat===selectedCat) el.classList.add('active'); else el.classList.remove('active');
        });
      }
      markActive();
      quickChips.addEventListener('click', (e)=>{
        const chip = e.target.closest('.chip');
        if(!chip) return;
        selectedCat = chip.dataset.cat;
        markActive();
        renderSuggestionsByCat();
      }, { once:false });

      // build frequent amounts chips (last 50 txs rounded)
      function renderSuggestionsByCat(){
        const recentByCat = txs.filter(t=>t.categoryId===selectedCat);
        const recent = recentByCat.slice(0,80).map(t=>Math.abs(Number(t.amount)||0)).filter(v=>v>0);
        const rounded = recent.map(v=>{
          if(v>=1000) return Math.round(v/100)*100;
          if(v>=100) return Math.round(v/10)*10;
          return Math.round(v);
        });
        const top = Array.from(new Map(rounded.map(v=>[v,(recent.filter(x=>Math.round(x)===v).length)]))).sort((a,b)=>b[1]-a[1]).slice(0,6).map(([v])=>v);
        quickAmountChips.innerHTML = top.map(v=>`<span class="chip" data-amt="${v}">$${v}</span>`).join('');
        quickAmountChips.onclick = (e)=>{
          const chip = e.target.closest('.chip');
          if(!chip) return;
          quickAmount.value = chip.dataset.amt;
        };
        // notes
        const notes = recentByCat.map(t=> (t.note||'').trim()).filter(Boolean);
        const countsNote = new Map();
        notes.forEach(n=> countsNote.set(n, (countsNote.get(n)||0)+1));
        const topNotes = Array.from(countsNote.entries()).sort((a,b)=>b[1]-a[1]).slice(0,8).map(([n])=>n);
        quickNoteChips.innerHTML = topNotes.map(n=>`<span class="chip" data-note="${n.replace(/"/g,'&quot;')}">${n}</span>`).join('');
        quickNoteChips.onclick = (e)=>{
          const chip = e.target.closest('.chip');
          if(!chip) return;
          quickNote.value = chip.dataset.note;
        };
      }
      renderSuggestionsByCat();

      // reset quick form
      setQuickType('expense');
      quickAmount.value = '';
      quickNote.value = '';
      if(quickDialog && typeof quickDialog.showModal === 'function'){
        quickDialog.showModal();
      }else if(quickDialog){
        try{ quickDialog.show?.(); }catch(_){ /* some browsers */ }
        if(!quickDialog.open) quickDialog.setAttribute('open','');
      }

      quickForm.onsubmit = async (ev)=>{
        // 支援對話框「取消」按鈕：不要攔截預設關閉
        const submitter = ev.submitter;
        if(submitter && submitter.value === 'cancel'){
          ev.preventDefault();
          try{ quickDialog.close?.(); }catch(_){ }
          return;
        }
        ev.preventDefault();
        const amount = Number(quickAmount.value);
        if(!Number.isFinite(amount) || amount<=0){
          alert('請輸入金額');
          return;
        }
        const payload = {
          date: today(),
          type: quickType,
          categoryId: selectedCat,
          currency: 'TWD',
          rate: 1,
          amount,
          note: quickNote.value.trim()
        };
        await DB.addTransaction(payload);
        if(!(quickKeepOpenToggle?.checked)){
          quickDialog.close();
        }else{
          quickAmount.value = '';
          quickNote.value = '';
        }
        refresh();
      };
      // 明確綁定「取消」按鈕關閉（避免瀏覽器差異）
      const quickCancelBtn = quickDialog?.querySelector('button[value="cancel"]');
      quickCancelBtn?.addEventListener('click', (e)=>{
        try{ e.preventDefault(); }catch(_){ }
        try{ quickDialog.close?.(); }catch(_){ quickDialog.removeAttribute?.('open'); }
      });
      // 點擊遮罩關閉
      quickDialog?.addEventListener('click', (e)=>{
        if(e.target === quickDialog){ try{ quickDialog.close?.(); }catch(_){ } }
      });
    });

    const currencySel = $('#txCurrency');
    const rateInput = $('#txRate');
    const updateBtn = $('#updateRateBtn');
    async function fetchRateToTWD(currency){
      if(!currency || currency==='TWD'){
        rateInput.value = '1';
        return;
      }
      try{
        // Use a free endpoint (no key). If blocked, user can input manually.
        const res = await fetch(`https://api.exchangerate.host/latest?base=${encodeURIComponent(currency)}&symbols=TWD`);
        if(!res.ok) throw new Error('Network');
        const json = await res.json();
        const r = json && json.rates && json.rates.TWD;
        if(r){ rateInput.value = String(r.toFixed(4)); }
      }catch(err){
        alert('取得匯率失敗，請稍後重試或手動輸入');
      }
    }
    if(currencySel){
      currencySel.addEventListener('change', ()=>{
        fetchRateToTWD(currencySel.value);
      });
    }
    if(updateBtn){
      updateBtn.addEventListener('click', ()=>{
        fetchRateToTWD(currencySel ? currencySel.value : 'TWD');
      });
    }

    // Settings dialog
    const settingsBtn = $('#settingsBtn');
    const settingsDialog = $('#settingsDialog');
    const saveSettingsBtn = $('#saveSettingsBtn');
    if(settingsBtn && settingsDialog){
      settingsBtn.addEventListener('click', async ()=>{
        const s = (await DB.getSettings?.()) || { baseCurrency:'TWD', monthlyBudgetTWD:0, savingsGoalTWD:0, nudges:true, appearance:'system', categoryBudgets:{} };
        $('#monthlyBudget').value = s.monthlyBudgetTWD||0;
        $('#savingsGoal').value = s.savingsGoalTWD||0;
        $('#nudgesToggle').checked = !!s.nudges;
        if($('#appearanceSelect')) $('#appearanceSelect').value = s.appearance || 'system';
        if($('#serverUrl')) $('#serverUrl').value = s.serverUrl || '';
        // render per-category budgets
        try{
          const list = $('#categoryBudgetList');
          if(list){
            const cats = await DB.getCategories();
            const budgets = s.categoryBudgets||{};
            list.innerHTML = cats.map(c=>`<div class="inline" style="justify-content:space-between; padding:6px 0; border-bottom:1px dashed var(--glass-border)">
              <span>${c.name}</span>
              <input type="number" step="0.01" inputmode="decimal" placeholder="0" value="${budgets[c.id]||0}" data-cat="${c.id}">
            </div>`).join('');
          }
        }catch(_){ /* ignore */ }
        showDialogSafe(settingsDialog);
      });
    }
    // live preview when changing appearance select
    const appearanceSelect = $('#appearanceSelect');
    appearanceSelect?.addEventListener('change', ()=>{
      applyAppearance(appearanceSelect.value);
    });

    if(saveSettingsBtn){
      saveSettingsBtn.addEventListener('click', async ()=>{
        const budgets = {};
        $$('#categoryBudgetList input[data-cat]').forEach(inp=>{ budgets[inp.dataset.cat] = Number(inp.value)||0; });
        const serverVal = $('#serverUrl') ? $('#serverUrl').value.trim() : '';
        if(serverVal){ try{ localStorage.setItem('serverUrl', serverVal); }catch(_){ } }
        await DB.setSettings?.({
          monthlyBudgetTWD: Number($('#monthlyBudget').value)||0,
          savingsGoalTWD: Number($('#savingsGoal').value)||0,
          nudges: $('#nudgesToggle').checked,
          appearance: $('#appearanceSelect') ? $('#appearanceSelect').value : 'system',
          serverUrl: $('#serverUrl') ? $('#serverUrl').value.trim() : '',
          categoryBudgets: budgets
        });
        settingsDialog.close();
        computeInsights();
        const s2 = await DB.getSettings?.();
        applyAppearance(s2?.appearance||'system');
      });
    }

    // NLP quick add
    const nlpInput = $('#nlpInput');
    const parseBtn = $('#parseNlpBtn');
    const quickAddBtn = $('#quickAddNlpBtn');
    parseBtn?.addEventListener('click', ()=>{
      const parsed = parseNlp(nlpInput.value||'');
      if(parsed.note){ $('#txNote').value = parsed.note; }
      if(parsed.date){ $('#txDate').value = parsed.date; }
      if(parsed.type){ $('#txType').value = parsed.type; }
      if(parsed.currency){ $('#txCurrency').value = parsed.currency; }
      if(parsed.rate){ $('#txRate').value = parsed.rate; }
      if(Number.isFinite(parsed.amount)){ $('#txAmount').value = parsed.amount; }
      if(Number.isFinite(parsed.claimAmount)){ $('#txClaimAmount').value = parsed.claimAmount; }
      if(typeof parsed.claimed==='boolean'){ $('#txClaimed').checked = parsed.claimed; }
      suggestCategory();
    });
    quickAddBtn?.addEventListener('click', async ()=>{
      const parsed = parseNlp(nlpInput.value||'');
      const payload = {
        date: parsed.date || $('#txDate').value,
        type: parsed.type || $('#txType').value,
        categoryId: $('#txCategory').value,
        currency: parsed.currency || ($('#txCurrency')?$('#txCurrency').value:'TWD'),
        rate: Number(parsed.rate || ($('#txRate')?$('#txRate').value:1))||1,
        amount: Number(parsed.amount ?? $('#txAmount').value),
        claimAmount: Number(parsed.claimAmount ?? ($('#txClaimAmount')?$('#txClaimAmount').value:0))||0,
        claimed: typeof parsed.claimed==='boolean' ? parsed.claimed : ($('#txClaimed')?$('#txClaimed').checked:false),
        note: parsed.note || $('#txNote').value.trim()
      };
      if(!payload.date || !payload.categoryId || !payload.type || !Number.isFinite(payload.amount)){
        alert('解析不足，請補齊欄位');
        return;
      }
      await DB.addTransaction(payload);
      $('#txForm').reset();
      $('#txDate').value = today();
      refresh();
    });

    // AI Dialog
    const openAIDialogBtn = $('#openAIDialogBtn');
    const aiDialog = $('#aiDialog');
    const aiForm = $('#aiForm');
    const aiMessage = $('#aiMessage');
    const aiAnswer = $('#aiAnswer');
    const aiAutoAddToggle = $('#aiAutoAddToggle');
    const aiForceAiToggle = $('#aiForceAiToggle');
    const aiPreview = $('#aiPreview');
    let aiAbort = null; // AbortController for in-flight AI requests
    openAIDialogBtn?.addEventListener('click', ()=>{
      aiAnswer.innerHTML = '';
      aiMessage.value = '';
      aiDialog.showModal();
    });
    const aiCancelBtn = $('#aiCancelBtn');
    aiCancelBtn?.addEventListener('click', ()=>{
      try{ aiAbort?.abort(); }catch(_){ }
      aiDialog.close();
    });
    // click backdrop to close
    aiDialog?.addEventListener('click', (e)=>{
      if(e.target === aiDialog){ try{ aiAbort?.abort(); }catch(_){ } aiDialog.close(); }
    });
    // ESC to close (cancel event)
    aiDialog?.addEventListener('cancel', ()=>{ try{ aiAbort?.abort(); }catch(_){ } });
    aiForm?.addEventListener('submit', async (e)=>{
      e.preventDefault();
      // If the submitter is the cancel button, just close the dialog
      const submitter = e.submitter;
      if(submitter && submitter.value === 'cancel'){
        aiDialog.close();
        return;
      }
      const text = aiMessage.value.trim();
      if(!text) return;
      aiAnswer.textContent = '思考中…';
      const context = {
        settings: await DB.getSettings?.(),
        transactions: await DB.getTransactions(),
        categories: await DB.getCategories()
      };
      async function structParseOne(base, ctx, line){
        try{
          const resp = await fetch(`${base.replace(/\/$/,'')}/api/ai`,{
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ messages:[{ role:'user', content:line }], context: ctx, mode:'struct' }),
            signal: aiAbort.signal
          });
          const data = await resp.json().catch(()=>({ ok:false }));
          if(resp.ok && data?.parsed){ return data.parsed; }
          return null;
        }catch(_){ return null; }
      }
      function guessCategoryFromText(text){
        try{
          const s = String(text||'').trim();
          if(!s) return '';
          const cleaned = s.replace(/\s+/g,'');
          const m = cleaned.match(/^[^0-9$￥¥.,，。；;]+/);
          const word = (m && m[0]) ? m[0].slice(0, 12) : '';
          return word;
        }catch(_){ return ''; }
      }
      async function buildPayloadFromParsed(parsed, localFallback, fallbackNote, prevDate){
        const p = parsed||{};
        const lf = localFallback||{};
        let catId = p.categoryId || '';
        // 0) 如果 AI 結構化提供了 categoryName，優先映射到現有分類 id
        if(!catId && p.categoryName){
          try{
            const cats = await DB.getCategories();
            const hit = cats.find(c=> String(c.name).toLowerCase()===String(p.categoryName).toLowerCase());
            if(hit) catId = hit.id;
            // 找不到則自動建立新分類
            if(!catId){
              const created = await DB.addCategory(String(p.categoryName).trim());
              if(created && created.id){ catId = created.id; }
            }
          }catch(_){ /* ignore */ }
        }
        if(!catId){
          try{
            // 1) model-based suggestion
            catId = await DB.suggestCategoryFromNote?.(p.note||fallbackNote||'') || '';
            // 2) fuzzy match category name contained in note/text
            if(!catId){
              const cats = await DB.getCategories();
              const noteLower = String(p.note||fallbackNote||'').toLowerCase();
              const hit = cats.find(c=> noteLower.includes(String(c.name||'').toLowerCase()));
              if(hit) catId = hit.id;
            }
          }catch(_){ }
        }
        // 3) 仍無分類：從文字猜測並建立新分類
        if(!catId){
          const guess = guessCategoryFromText(p.note||fallbackNote||'');
          if(guess){
            try{ const created = await DB.addCategory(guess); if(created && created.id){ catId = created.id; } }catch(_){ }
          }
        }
        // amount/date 以本地解析為優先（避免 AI 誤判），AI 補足缺漏
        const finalAmount = Number.isFinite(Number(lf.amount)) ? Number(lf.amount) : Number(p.amount)||0;
        const finalDate = lf.date || p.date || prevDate || today();
        const payload = {
          date: finalDate,
          type: p.type || lf.type || 'expense',
          categoryId: catId || ($('#txCategory')?.value || ''),
          currency: p.currency || 'TWD',
          rate: Number(p.rate)||1,
          amount: finalAmount||0,
          claimAmount: Number.isFinite(Number(lf.claimAmount)) ? Number(lf.claimAmount) : Number(p.claimAmount)||0,
          claimed: p.claimed===true,
          note: (p.note||lf.note||fallbackNote||'').trim()
        };
        return payload;
      }
      async function batchAdd(base, ctx, raw){
        const lines = String(raw).split(/\r\n|\n|\r|[;；]/).map(s=>s.trim()).filter(Boolean);
        if(lines.length<=1) return false;
        aiAnswer.textContent = '批次處理中…';
        let success=0, skipped=0;
        let lastDate = '';
        for(const line of lines){
          // 1) AI struct
          let parsed = aiForceAiToggle?.checked ? await structParseOne(base, ctx, line) : null;
          // 2) local parse fallback
          const localParsed = parseNlp(line);
          if(!parsed){ parsed = localParsed; }
          // 3) must have amount
          const haveAmount = Number.isFinite(Number(localParsed?.amount)) || Number.isFinite(Number(parsed?.amount));
          if(!haveAmount){ skipped++; continue; }
          const payload = await buildPayloadFromParsed(parsed, localParsed, line, lastDate);
          if(!payload.categoryId || !payload.type || !Number.isFinite(payload.amount) || payload.amount<=0){ skipped++; continue; }
          try{ await DB.addTransaction(payload); success++; }
          catch(_){ skipped++; }
          if(payload.date){ lastDate = payload.date; }
        }
        aiAnswer.textContent = `已新增 ${success} 筆，略過 ${skipped} 筆。`;
        if(success>0){ refresh(); setTimeout(()=>{ try{ aiDialog.close(); }catch(_){ } }, 600); }
        return true;
      }
      async function streamChat(base, contextObj, userText){
        try{
          aiAnswer.innerHTML = '<span class="ai-typing"><i></i><i></i><i></i></span>';
          const resp2 = await fetch(`${base.replace(/\/$/,'')}/api/ai/stream`,{
            method:'POST', headers:{ 'Content-Type':'application/json' },
            body: JSON.stringify({ messages:[{ role:'user', content:userText }], context: contextObj }),
            signal: aiAbort.signal
          });
          if(!resp2.ok || !resp2.body){ return false; }
          const reader = resp2.body.getReader();
          const decoder = new TextDecoder();
          let buffer = '';
          let started = false;
          while(true){
            const { done, value } = await reader.read();
            if(done) break;
            buffer += decoder.decode(value, { stream:true });
            const parts = buffer.split(/\n\n/);
            buffer = parts.pop()||'';
            for(const chunk of parts){
              const line = chunk.trim();
              if(!line) continue;
              const m = line.match(/^data:\s*(.*)$/m);
              const payload = m ? m[1] : '';
              if(!payload) continue;
              if(payload === '[DONE]') continue;
              try{
                const obj = JSON.parse(payload);
                if(obj && obj.delta){
                  if(!started){ aiAnswer.textContent=''; started=true; }
                  aiAnswer.textContent += obj.delta;
                }
                if(obj && obj.done){ return true; }
                if(obj && obj.error){ return false; }
              }catch(_){ /* ignore */ }
            }
          }
          return true;
        }catch(err){ return false; }
      }
      async function autoAddFromText(t){
        const parsed = parseNlp(t);
        // require amount
        if(!Number.isFinite(parsed.amount)) return false;
        let catId = parsed.categoryId || '';
        if(!catId){
          try{
            // 1) model-based suggestion
            catId = await DB.suggestCategoryFromNote?.(parsed.note||'') || '';
            // 2) fuzzy match category name contained in note/text
            if(!catId){
              const cats = await DB.getCategories();
              const noteLower = String(parsed.note||t||'').toLowerCase();
              const hit = cats.find(c=> noteLower.includes(String(c.name||'').toLowerCase()));
              if(hit) catId = hit.id;
            }
          }catch(_){ /* ignore */ }
        }
        // 3) still none: create category from guess
        if(!catId){
          const guess = guessCategoryFromText(parsed.note||t||'');
          if(guess){
            try{ const created = await DB.addCategory(guess); if(created && created.id){ catId = created.id; } }catch(_){ }
          }
        }
        const payload = {
          date: parsed.date || today(),
          type: parsed.type || ($('#txType')?.value || 'expense'),
          categoryId: catId || ($('#txCategory')?.value || ''),
          currency: parsed.currency || 'TWD',
          rate: Number(parsed.rate)||1,
          amount: Number(parsed.amount),
          claimAmount: Number(parsed.claimAmount)||0,
          claimed: typeof parsed.claimed==='boolean' ? parsed.claimed : false,
          note: (parsed.note||'').trim()
        };
        if(!payload.categoryId || !payload.type || !Number.isFinite(payload.amount)) return false;
        await DB.addTransaction(payload);
        refresh();
        aiAnswer.textContent = `已新增：${payload.type==='income'?'收入':'支出'} $${formatAmount(payload.amount)}（${payload.currency||'TWD'}）`;
        setTimeout(()=>{ try{ aiDialog.close(); }catch(_){ } }, 600);
        return true;
      }
      try{
        const s = await DB.getSettings?.();
        const candidate = ($('#serverUrl')?.value || s?.serverUrl || '').trim();
        // Prefer same-origin; then user-configured; then localhost default
        const base = apiBase(candidate);
        aiAbort = new AbortController();
        // Multi-line batch mode（以多種換行/分隔符判斷）
        const parts = String(text).split(/\r\n|\n|\r|[;；]/).map(s=>s.trim()).filter(Boolean);
        if(parts.length>1){
          const done = await batchAdd(base, context, text);
          if(done) return;
        }
        // 先請 AI 進行結構化解析
        const resp = await fetch(`${base.replace(/\/$/,'')}/api/ai`,{
          method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({ messages:[{ role:'user', content:text }], context, mode:'struct' }),
          signal: aiAbort.signal
        });
        const data = await resp.json().catch(()=>({ ok:false, error:'invalid_json' }));
        if(resp.ok && data?.parsed){
          // 顯示預覽，依開關自動新增
          const p = data.parsed;
          aiPreview.style.display='block';
          aiPreview.innerHTML = `<div>解析結果：${p.type||'expense'} $${p.amount} ${p.currency||'TWD'} ・ ${p.categoryName||''} ・ ${p.date||today()}<br><small>${(p.note||'').slice(0,100)}</small></div>`;
          if(aiAutoAddToggle?.checked){
            const cats = await DB.getCategories();
            const hit = (p.categoryName && cats.find(c=> String(c.name).toLowerCase()===String(p.categoryName).toLowerCase()))?.id;
            let catId = hit || '';
            if(!catId && p.categoryName){
              try{ const created = await DB.addCategory(String(p.categoryName).trim()); if(created && created.id){ catId = created.id; } }catch(_){ }
            }
            const payload = {
              date: p.date || today(),
              type: p.type || 'expense',
              categoryId: catId || $('#txCategory')?.value || '',
              currency: p.currency || 'TWD', rate: Number(p.rate)||1,
              amount: Number(p.amount)||0,
              claimAmount: Number(p.claimAmount)||0,
              claimed: p.claimed===true,
              note: (p.note||'').trim()
            };
            if(payload.categoryId && payload.type && Number.isFinite(payload.amount) && payload.amount>0){
              await DB.addTransaction(payload);
              refresh();
              aiAnswer.textContent = '已自動記帳';
              setTimeout(()=>{ try{ aiDialog.close(); }catch(_){ } }, 600);
              return;
            }
          }
        } else if(!resp.ok || data?.ok===false){
          // fallback：本地解析直接新增
          const ok = await autoAddFromText(text);
          if(!ok){ aiAnswer.textContent = `AI 失敗：${data?.error||resp.status}`; }
          return; 
        }
        // 如果供應商回覆文字，仍嘗試本地解析新增（以達成你要的自動記帳）
        const ok = await autoAddFromText(text);
        if(!ok){
          // 串流回覆（逐字顯示）；若串流不可用則回退一般回覆
          const streamed = await streamChat(base, context, text);
          if(!streamed){ aiAnswer.textContent = data?.reply || '沒有回覆'; }
        }
      }catch(err){
        // 網路錯誤時改用本地解析
        const ok = await autoAddFromText(text);
        if(!ok){ aiAnswer.textContent = `呼叫 AI 失敗：${String(err?.message||err)}`; }
      }finally{
        aiAbort = null;
      }
    });
    // Category suggestion when note changes
    $('#txNote')?.addEventListener('input', ()=>{ suggestCategory(); });
  }

  function parseNlp(text){
    const t = String(text||'').trim();
    const result = { };
    const normalized = t.replace(/，/g, ',');
    // type
    if(/(^|\s)(支出|花費|付|花|扣)($|\s)/.test(t)) result.type='expense';
    if(/(^|\s)(收入|入帳|收)($|\s)/.test(t)) result.type='income';
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
    // amount (supports chinese numerals)
    function chineseToNumber(input){
      if(!input) return NaN;
      const digit = { '零':0,'〇':0,'一':1,'二':2,'兩':2,'三':3,'四':4,'五':5,'六':6,'七':7,'八':8,'九':9 };
      const unit = { '十':10,'百':100,'千':1000,'萬':10000 };
      let total = 0, section = 0, number = 0;
      for(const ch of input){
        if(digit.hasOwnProperty(ch)) number = digit[ch];
        else if(unit.hasOwnProperty(ch)){
          const u = unit[ch];
          if(u===10000){ section += (number||0); total += section*10000; section=0; number=0; }
          else { section += (number||1)*u; number=0; }
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
    const amt = parseAmount(normalized);
    if(Number.isFinite(amt)) result.amount = amt;
    // currency strict code fallback
    if(!result.currency){ const cur = t.match(/\b(TWD|USD|JPY|EUR|CNY|HKD)\b/i); if(cur) result.currency = cur[1].toUpperCase(); }
    // rate keyword
    const rate = t.match(/匯率\s*([0-9]+(?:\.[0-9]+)?)/);
    if(rate) result.rate = Number(rate[1]);
    // date ISO or relative
    const date = t.match(/(20\d{2})[-\/](\d{1,2})[-\/](\d{1,2})/);
    if(date){ const y=Number(date[1]); const m=String(Number(date[2])).padStart(2,'0'); const d=String(Number(date[3])).padStart(2,'0'); result.date=`${y}-${m}-${d}`; }
    if(!result.date){
      // 支援 MM/DD（無年份）→ 以今年為準
      const md = t.match(/\b(\d{1,2})\/(\d{1,2})\b/);
      if(md){
        const now = new Date();
        const y = now.getFullYear();
        const m = String(Number(md[1])).padStart(2,'0');
        const d = String(Number(md[2])).padStart(2,'0');
        result.date = `${y}-${m}-${d}`;
      }
      const now = new Date();
      const fmt = (d)=> `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
      if(/今天/.test(t)) result.date = fmt(new Date());
      else if(/昨天|昨日/.test(t)){ const d=new Date(now); d.setDate(d.getDate()-1); result.date = fmt(d); }
      else if(/前天/.test(t)){ const d=new Date(now); d.setDate(d.getDate()-2); result.date = fmt(d); }
      else if(/明天/.test(t)){ const d=new Date(now); d.setDate(d.getDate()+1); result.date = fmt(d); }
    }
    // claimed
    if(/已請款|完成請款|報帳完成/.test(t)) result.claimed = true;
    if(/未請款|還沒請款/.test(t)) result.claimed = false;
    // emotion keywords
    if(/開心|快樂|爽/.test(t)) result.emotion='happy';
    if(/中立|還好/.test(t)) result.emotion='neutral';
    if(/壓力|焦慮|煩/.test(t)) result.emotion='stress';
    if(/難過|低落|傷心/.test(t)) result.emotion='sad';
    if(/後悔|不該/.test(t)) result.emotion='regret';
    // motivation hints
    const m = t.match(/(社交|舒壓|健康|工作|學習|家庭|旅遊|投資|娛樂)/);
    if(m) result.motivation = m[1];
    // note (full text)
    result.note = t;
    // claim amount explicit
    const camt = t.match(/請款\s*([0-9]+(?:\.[0-9]+)?)/);
    if(camt) result.claimAmount = Number(camt[1]);
    return result;
  }

  async function suggestCategory(){
    const suggestionWrap = $('#categorySuggestion');
    if(!suggestionWrap) return;
    const note = $('#txNote').value || '';
    const catId = await DB.suggestCategoryFromNote?.(note);
    const categories = await DB.getCategories();
    const chips = categories.map(c=>{
      const active = c.id===catId ? ' style="border-color:#0ea5e9;color:#0ea5e9"' : '';
      return `<span class="chip" data-cat="${c.id}"${active}>${c.name}</span>`;
    }).join('');
    suggestionWrap.innerHTML = chips;
    suggestionWrap.querySelectorAll('.chip').forEach(el=>{
      el.addEventListener('click', ()=>{
        $('#txCategory').value = el.dataset.cat;
      });
    });
  }

  async function computeInsights(){
    const list = $('#insightsList');
    if(!list) return;
    const [settings, txs] = await Promise.all([
      DB.getSettings?.(),
      DB.getTransactions()
    ]);
    const now = new Date();
    const ym = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
    const monthTx = txs.filter(t=> (t.date||'').startsWith(ym));
    const spent = monthTx.filter(t=>t.type==='expense').reduce((s,t)=> s + toBaseCurrency(t.amount, t.currency||'TWD', t.rate||1), 0);
    const budget = settings?.monthlyBudgetTWD || 0;
    const unclaimed = txs.filter(t=> t.type==='expense' && !t.claimed);
    const streak = (()=>{
      // count consecutive days with any transaction up to today
      const set = new Set(txs.map(t=>t.date));
      let d = new Date();
      let cnt = 0;
      while(set.has(`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`)){
        cnt++; d.setDate(d.getDate()-1);
      }
      return cnt;
    })();
    const items = [];
    if(budget>0){
      const ratio = spent / budget;
      if(ratio>=1) items.push(`本月已超出預算：$${formatAmount(spent)} / $${formatAmount(budget)}`);
      else if(ratio>=0.8) items.push(`接近預算：已花 $${formatAmount(spent)}，預算 $${formatAmount(budget)}`);
    }
    // per-category budgets
    const per = (settings?.categoryBudgets)||{};
    const mapCatSpent = new Map();
    for(const t of monthTx){
      if(t.type!=='expense') continue;
      const v = toBaseCurrency(t.amount, t.currency||'TWD', t.rate||1);
      mapCatSpent.set(t.categoryId, (mapCatSpent.get(t.categoryId)||0) + v);
    }
    const cats = await DB.getCategories();
    for(const [catId, limit] of Object.entries(per)){
      const used = mapCatSpent.get(catId)||0;
      if(limit>0){
        if(used>=limit){
          const name = cats.find(c=>c.id===catId)?.name || catId;
          items.push(`分類「${name}」已超支：$${formatAmount(used)} / $${formatAmount(limit)}`);
        }else if(used>=limit*0.8){
          const name = cats.find(c=>c.id===catId)?.name || catId;
          items.push(`分類「${name}」接近預算：$${formatAmount(used)} / $${formatAmount(limit)}`);
        }
      }
    }
    if(unclaimed.length>0) items.push(`有 ${unclaimed.length} 筆未請款，記得報帳`);
    if(streak>=3) items.push(`已連續 ${streak} 天有紀錄，太棒了！`);
    list.innerHTML = items.map(x=>`<li>${x}</li>`).join('') || '<li>目前沒有特別提醒</li>';
  }

  function groupByMonth(items){
    return items.reduce((map, t)=>{
      const key = (t.date||'').slice(0,7); // YYYY-MM
      if(!map.has(key)) map.set(key, []);
      map.get(key).push(t);
      return map;
    }, new Map());
  }

  function groupByYear(items){
    return items.reduce((map, t)=>{
      const key = (t.date||'').slice(0,4); // YYYY
      if(!map.has(key)) map.set(key, []);
      map.get(key).push(t);
      return map;
    }, new Map());
  }

  function renderChart(){
    const modeSel = $('#statsMode');
    const root = $('#statsChart');
    const details = $('#statsDetails');
    if(!modeSel || !root){ return; }
    const mode = modeSel.value;
    DB.getTransactions().then(items=>{
      const y = String(new Date().getFullYear());
      const curYear = items.filter(t=>(t.date||'').startsWith(y));
      const byMonth = groupByMonth(curYear);
      const byYear = groupByYear(items);
      function sum(arr, pred){ return arr.filter(pred).reduce((s,t)=> s+toBaseCurrency(t.amount, t.currency||'TWD', t.rate||1), 0); }
      function renderBars(series){
        const max = Math.max(1, ...series.map(d=>Math.abs(d.value)));
        root.innerHTML = series.map(d=>{
          const h = Math.round((Math.abs(d.value) / max) * 180) + 8;
          const cls = d.value<0 ? 'bar negative' : 'bar';
          const title = `${d.label}: $${formatAmount(d.value)}`;
          return `<div title="${title}" class="${cls}" style="height:${h}px"></div>`;
        }).join('');
      }
      function renderDetails(rows){
        if(!details) return;
        details.innerHTML = rows.map(r=>`<div>${r}</div>`).join('');
      }

      if(mode==='yearly'){
        const entries = Array.from(byYear.entries()).sort((a,b)=>a[0].localeCompare(b[0]));
        const series = entries.map(([label, arr])=>({ label, value: sum(arr,t=>t.type==='income') - sum(arr,t=>t.type==='expense') }));
        renderBars(series);
        renderDetails(entries.map(([label, arr])=>{
          const inc = sum(arr,t=>t.type==='income'); const exp = sum(arr,t=>t.type==='expense');
          return `${label}｜收入 $${formatAmount(inc)}｜支出 $${formatAmount(exp)}｜結餘 $${formatAmount(inc-exp)}`;
        }));
        return;
      }

      if(mode==='monthly_income' || mode==='monthly_expense' || mode==='monthly'){
        const entries = Array.from(byMonth.entries()).sort((a,b)=>a[0].localeCompare(b[0]));
        const series = entries.map(([label, arr])=>{
          const inc = sum(arr,t=>t.type==='income');
          const exp = sum(arr,t=>t.type==='expense');
          const val = (mode==='monthly_income') ? inc : (mode==='monthly_expense' ? exp : (inc-exp));
          return { label, value: val*(mode==='monthly_expense'?-1:1) };
        });
        renderBars(series);
        renderDetails(entries.map(([label, arr])=>{
          const inc = sum(arr,t=>t.type==='income'); const exp = sum(arr,t=>t.type==='expense');
          return `${label}｜收入 $${formatAmount(inc)}｜支出 $${formatAmount(exp)}｜結餘 $${formatAmount(inc-exp)}`;
        }));
        return;
      }

      if(mode==='monthly_category'){
        const ym = `${y}-${String(new Date().getMonth()+1).padStart(2,'0')}`;
        const monthTx = curYear.filter(t=> (t.date||'').startsWith(ym) && t.type==='expense');
        const byCat = monthTx.reduce((m,t)=>{ m.set(t.categoryId, (m.get(t.categoryId)||0)+toBaseCurrency(t.amount,t.currency||'TWD',t.rate||1)); return m; }, new Map());
        const rows = Array.from(byCat.entries()).sort((a,b)=>b[1]-a[1]);
        const series = rows.map(([id,v])=>({ label:id, value: -v }));
        renderBars(series);
        renderDetails(rows.map(([id,v])=> `${id}：$${formatAmount(v)}`));
        return;
      }

      if(mode==='monthly_cumulative'){
        const entries = Array.from(byMonth.entries()).sort((a,b)=>a[0].localeCompare(b[0]));
        let acc = 0; const series = entries.map(([label, arr])=>{ acc += sum(arr,t=>t.type==='income') - sum(arr,t=>t.type==='expense'); return { label, value: acc }; });
        renderBars(series);
        renderDetails(entries.map(([label, arr],i)=> `${label}：累積 $${formatAmount(series[i].value)}`));
        return;
      }

      if(mode==='unclaimed'){
        const pending = curYear.filter(t=> t.type==='expense' && t.claimed!==true);
        const sumPending = sum(pending, ()=>true);
        renderBars([ { label:'未請款', value: -sumPending } ]);
        renderDetails([ `未請款筆數：${pending.length}｜金額：$${formatAmount(sumPending)}` ]);
        return;
      }
    });
  }

  function clearEditingState(){
    const form = $('#txForm');
    delete form.dataset.editingId;
    $('#submitBtn').textContent = '新增';
    $('#cancelEditBtn').style.display = 'none';
    form.reset();
    $('#txDate').value = today();
  }

  function startOfMonth(date){ return new Date(date.getFullYear(), date.getMonth(), 1); }
  function endOfMonth(date){ return new Date(date.getFullYear(), date.getMonth()+1, 0); }
  function ymd(d){ const m=String(d.getMonth()+1).padStart(2,'0'); const day=String(d.getDate()).padStart(2,'0'); return `${d.getFullYear()}-${m}-${day}`; }

  let calendarCursor = new Date();
  let dateFilter = null; // YYYY-MM-DD or null

  function renderCalendar(items){
    const root = $('#calendarGrid');
    const label = $('#calMonthLabel');
    if(!root || !label) return;
    const s = startOfMonth(calendarCursor);
    const e = endOfMonth(calendarCursor);
    label.textContent = `${s.getFullYear()}-${String(s.getMonth()+1).padStart(2,'0')}`;
    // build map of daily base currency net
    const map = new Map();
    for(const t of items){
      if(!t.date) continue;
      const d = t.date;
      const base = toBaseCurrency(t.amount, t.currency||'TWD', t.rate||1) * (t.type==='income'?1:-1);
      map.set(d, (map.get(d)||0) + base);
    }
    // compute grid: leading blanks
    const startWeekday = s.getDay(); // 0-6
    const days = e.getDate();
    const cells = [];
    for(let i=0;i<startWeekday;i++){ cells.push(null); }
    for(let d=1; d<=days; d++){
      cells.push(new Date(s.getFullYear(), s.getMonth(), d));
    }
    root.innerHTML = cells.map(c=>{
      if(!c) return `<div class="cal-cell"></div>`;
      const key = ymd(c);
      const val = map.get(key)||0;
      const cls = val>0 ? 'income' : (val<0 ? 'expense' : '');
      const todayFlag = (key===ymd(new Date())) ? ' today' : '';
      return `<div class="cal-cell${todayFlag} ${cls}"><button class="cal-cellbutton" data-date="${key}"><div class="day">${c.getDate()}</div><div class="sum">$${formatAmount(Math.abs(val))}</div></button></div>`;
    }).join('');
  }

  function bindCalendarEvents(){
    const prev = $('#calPrevBtn');
    const next = $('#calNextBtn');
    const clearBtn = $('#clearDateFilterBtn');
    if(prev) prev.addEventListener('click', ()=>{ calendarCursor = new Date(calendarCursor.getFullYear(), calendarCursor.getMonth()-1, 1); refresh(); });
    if(next) next.addEventListener('click', ()=>{ calendarCursor = new Date(calendarCursor.getFullYear(), calendarCursor.getMonth()+1, 1); refresh(); });
    if(clearBtn) clearBtn.addEventListener('click', ()=>{ dateFilter = null; filterAndRender(); });
    const grid = $('#calendarGrid');
    if(grid){
      grid.addEventListener('click', (e)=>{
        const btn = e.target.closest('button[data-date]');
        if(!btn) return;
        dateFilter = btn.dataset.date;
        filterAndRender();
      });
    }
  }

  async function main(){
    // Require login if backend enforces auth. Show login/logout UI.
    const loginBtn = document.getElementById('loginBtn');
    const logoutBtn = document.getElementById('logoutBtn');
    const userName = document.getElementById('userName');
    async function refreshAuth(){
      try{
        const me = await fetch('/api/me').then(r=> r.ok ? r.json() : Promise.reject(r));
        if(me && me.ok){
          if(userName) userName.textContent = me.user?.name ? `您好，${me.user.name}` : '';
          if(loginBtn) loginBtn.style.display = 'none';
          if(logoutBtn) logoutBtn.style.display = '';
          return true;
        }
      }catch(_){ /* not logged in */ }
      if(userName) userName.textContent = '';
      if(loginBtn) loginBtn.style.display = '';
      if(logoutBtn) logoutBtn.style.display = 'none';
      return false;
    }
    loginBtn?.addEventListener('click', ()=>{ location.href = '/auth/line/start'; });
    logoutBtn?.addEventListener('click', async ()=>{
      try{ await fetch('/auth/logout', { method:'POST' }); }catch(_){ }
      await refreshAuth();
      // after logout, keep page but data操作會被擋。也可選擇導向登入。
    });
    const authed = await refreshAuth();
    if(!authed){
      // 未登入：嘗試探測後端是否允許匿名（REQUIRE_AUTH=false）
      let canAnonymousUse = false;
      try{
        const probe = await fetch('/api/categories');
        canAnonymousUse = probe.ok;
      }catch(_){ canAnonymousUse = false; }
      if(!canAnonymousUse){
        // 仍需登入，維持既有行為（顯示登入按鈕並停止初始化）
        return;
      }
    }
    $('#txDate').value = today();

    // 登入後：若之前使用本機 IndexedDB，嘗試把本機資料匯出並上傳至伺服器，之後切換成伺服器端資料源
    try{
      const hasServer = (typeof window!=='undefined') && window.DB_remote;
      const serverBase = (localStorage.getItem('serverUrl')||'').trim() || (location && location.origin) || '';
      if(hasServer && /^https?:\/\//.test(String(serverBase))){
        // 1) 先初始化本機 DB 以取得現有資料
        if(window.DB_idb){ try{ await window.DB_idb.init(); }catch(_){ /* ignore */ } }
        // 2) 匯出本機資料
        let localExport = null;
        try{ localExport = await (window.DB_idb ? window.DB_idb.exportAll() : (DB.exportAll?.()||null)); }catch(_){ }
        // 3) 切換到伺服器 provider
        if(window.DB_remote){ window.DB = window.DB_remote; }
        await DB.init?.();
        // 4) 若伺服器目前沒有資料而本機有，則上傳匯入一次
        try{
          const [serverCats, serverTxs] = await Promise.all([ DB.getCategories(), DB.getTransactions() ]);
          const serverEmpty = (Array.isArray(serverCats)&&serverCats.length<=1) && (Array.isArray(serverTxs)&&serverTxs.length===0);
          if(localExport && Array.isArray(localExport.transactions) && localExport.transactions.length>0 && serverEmpty){
            await DB.importAll(localExport);
          }
        }catch(_){ /* ignore sync errors */ }
      } else {
        // 無伺服器可用時，保持本機 DB
      }
    }catch(_){ /* ignore */ }

    await DB.init();
    bindEvents();
    bindCalendarEvents();
    refresh();
    // tabs
    const tabs = $$('.tab-btn');
    function showTab(name){
      const all = Array.from(document.querySelectorAll('[data-tab]'));
      all.forEach(el=>{
        const isTarget = el.getAttribute('data-tab')===name;
        if(isTarget){
          el.classList.remove('hidden');
          el.style.opacity='0'; el.style.transform='translateY(4px)';
          requestAnimationFrame(()=>{ el.style.opacity='1'; el.style.transform='translateY(0)'; });
        }else{
          el.classList.add('hidden');
          el.style.opacity=''; el.style.transform='';
        }
      });
      tabs.forEach(btn=>{
        if(btn.getAttribute('data-tab-target')===name) btn.classList.add('active'); else btn.classList.remove('active');
      });
    }
    tabs.forEach(btn=> btn.addEventListener('click', ()=> showTab(btn.getAttribute('data-tab-target'))));
    showTab('ledger');
    if($('#statsMode')){
      $('#statsMode').addEventListener('change', renderChart);
      renderChart();
    }
    computeInsights();
    // apply appearance on startup
    try{
      const s = await DB.getSettings?.();
      applyAppearance(s?.appearance||'system');
      if(window.matchMedia){
        window.matchMedia('(prefers-color-scheme: dark)').addEventListener?.('change', ()=>{
          const ss = DB.getSettings?.();
          Promise.resolve(ss).then(v=> applyAppearance((v&&v.appearance)||'system'));
        });
      }
    }catch(_){ applyAppearance('system'); }
    suggestCategory();
  }

  document.addEventListener('DOMContentLoaded', main);
})();
