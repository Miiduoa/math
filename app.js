(function(){
  const $ = (sel, root=document) => root.querySelector(sel);
  const $$ = (sel, root=document) => Array.from(root.querySelectorAll(sel));

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
      const claimedBadge = t.claimed ? '<span class="badge success">已請款</span>' : '';
      const base = toBaseCurrency(t.amount, t.currency||'TWD', t.rate||1);
      const amountText = `${t.currency||'TWD'} ${formatAmount(t.amount)}`;
      const claimText = (Number(t.claimAmount)||0) > 0 ? `<br><small>請款 ${t.currency||'TWD'} ${formatAmount(t.claimAmount)}</small>` : '';
      const emo = t.emotion ? `<div class="emotion">情緒：${t.emotion}${t.motivation?`｜動機：${t.motivation}`:''}</div>` : (t.motivation?`<div class="emotion">動機：${t.motivation}</div>`:'');
      return `<li class="tx-item" data-id="${t.id}">
        <div>
          <div>${t.note||'(無備註)'}・<small>${t.categoryName||t.categoryId}</small> ${claimedBadge}</div>${emo}
          <small>${t.date}</small>
        </div>
        <div class="tx-amount ${amountClass}">${sign}$${formatAmount(base)}<br><small>${amountText}</small>${claimText}</div>
        <div class="tx-actions">
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
    const [categories, txs] = await Promise.all([
      DB.getCategories(),
      DB.getTransactions()
    ]);
    const categoryMap = new Map(categories.map(c=>[c.id, c.name]));
    renderCategories(categories);
    const enriched = txs.map(t=>({ ...t, categoryName: categoryMap.get(t.categoryId)||t.categoryId }));
    renderTransactions(enriched);
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
    DB.getTransactions().then(items=>{
      let filtered = items;
      if(type!=='all') filtered = filtered.filter(t=>t.type===type);
      if(dateFilter){ filtered = filtered.filter(t=> (t.date||'') === dateFilter); }
      if(q){
        filtered = filtered.filter(t=>
          (t.note||'').toLowerCase().includes(q) || (t.categoryName||t.categoryId||'').toLowerCase().includes(q)
        );
      }
      renderTransactions(filtered);
    });
  }

  function bindEvents(){
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
    $('#importJsonInput').addEventListener('change', async (e)=>{
      const file = e.target.files?.[0];
      if(!file) return;
      const text = await file.text();
      try{
        const data = JSON.parse(text);
        const ok = await DB.importAll(data);
        if(!ok) alert('匯入失敗，格式不正確');
        await refresh();
      }catch(err){
        alert('匯入失敗，檔案非有效 JSON');
      }
      e.target.value = '';
    });

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
        if($('#serverUrl')) $('#serverUrl').value = s.serverUrl || 'http://localhost:8787';
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
        settingsDialog.showModal();
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
    openAIDialogBtn?.addEventListener('click', ()=>{
      aiAnswer.innerHTML = '';
      aiMessage.value = '';
      aiDialog.showModal();
    });
    aiForm?.addEventListener('submit', async (e)=>{
      e.preventDefault();
      const text = aiMessage.value.trim();
      if(!text) return;
      aiAnswer.textContent = '思考中…';
      const context = {
        settings: await DB.getSettings?.(),
        transactions: await DB.getTransactions(),
        categories: await DB.getCategories()
      };
      try{
        const serverUrl = $('#serverUrl')?.value || (await DB.getSettings?.())?.serverUrl || '';
        const base = serverUrl || location.origin;
        const resp = await fetch(`${base.replace(/\/$/,'')}/api/ai`,{
          method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({ messages:[{ role:'user', content:text }], context })
        });
        const data = await resp.json();
        aiAnswer.textContent = data?.reply || '沒有回覆';
      }catch(err){
        aiAnswer.textContent = '呼叫 AI 失敗，請稍後重試';
      }
    });
    // Category suggestion when note changes
    $('#txNote')?.addEventListener('input', ()=>{ suggestCategory(); });
  }

  function parseNlp(text){
    const t = String(text||'').trim();
    const result = { };
    // type
    if(/(^|\s)(支出|花費|付|花|扣)($|\s)/.test(t)) result.type='expense';
    if(/(^|\s)(收入|入帳|收)($|\s)/.test(t)) result.type='income';
    // amount
    const amt = t.match(/([0-9]+(?:\.[0-9]+)?)\s*(?:元|dollars|usd|jpy|eur|twd|cny|hkd)?/i);
    if(amt) result.amount = Number(amt[1]);
    // currency
    const cur = t.match(/\b(TWD|USD|JPY|EUR|CNY|HKD)\b/i);
    if(cur) result.currency = cur[1].toUpperCase();
    // rate keyword
    const rate = t.match(/匯率\s*([0-9]+(?:\.[0-9]+)?)/);
    if(rate) result.rate = Number(rate[1]);
    // date ISO
    const date = t.match(/(20\d{2})[-\/](\d{1,2})[-\/](\d{1,2})/);
    if(date){ const y=Number(date[1]); const m=String(Number(date[2])).padStart(2,'0'); const d=String(Number(date[3])).padStart(2,'0'); result.date=`${y}-${m}-${d}`; }
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
    if(!modeSel || !root){ return; }
    const mode = modeSel.value;
    DB.getTransactions().then(items=>{
      let groups;
      if(mode==='yearly'){
        groups = groupByYear(items);
      }else{
        const y = String(new Date().getFullYear());
        groups = groupByMonth(items.filter(t=>(t.date||'').startsWith(y)));
      }
      const entries = Array.from(groups.entries()).sort((a,b)=>a[0].localeCompare(b[0]));
      const data = entries.map(([label, arr])=>{
        const income = arr.filter(t=>t.type==='income').reduce((s,t)=>s+toBaseCurrency(t.amount, t.currency||'TWD', t.rate||1),0);
        const expense = arr.filter(t=>t.type==='expense').reduce((s,t)=>s+toBaseCurrency(t.amount, t.currency||'TWD', t.rate||1),0);
        return { label, value: Math.max(income-expense, 0) };
      });
      const max = Math.max(1, ...data.map(d=>d.value));
      root.innerHTML = data.map(d=>{
        const h = Math.round((d.value / max) * 180) + 8;
        return `<div title="${d.label}: $${formatAmount(d.value)}" class="bar" style="height:${h}px"></div>`;
      }).join('');
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
    $('#txDate').value = today();
    await DB.init();
    bindEvents();
    bindCalendarEvents();
    refresh();
    // tabs
    const tabs = $$('.tab-btn');
    function showTab(name){
      document.querySelectorAll('[data-tab]').forEach(el=>{
        if(el.getAttribute('data-tab')===name) el.classList.remove('hidden'); else el.classList.add('hidden');
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

