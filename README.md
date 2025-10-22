# 個人記帳系統（iOS 風格 + AI + 遠端後端）

## 特色
- 本機端離線可用（IndexedDB），資料匯入/匯出 JSON
- 多幣別與匯率（以 TWD 為本位）
- 交易：收入/支出、分類、備註、請款金額、已請款
- AI 助理：自然語言快速輸入、分類自動建議（持續學習）
- 洞察：預算超支提醒、未請款提醒、連續紀錄、月/年圖表
- 月曆檢視：每日總額（本位幣），點選即可篩選
- iOS 風格玻璃 UI

## 使用方式（本機預覽）
1. 直接以瀏覽器開啟 `index.html`
2. 新增交易於表單；可選幣別、更新匯率
3. 「AI 助理與洞察」可輸入自然語句，如：
   - 支出 120 餐飲 早餐 2025-10-12 USD 匯率 32 已請款
4. 「設定」可設定每月預算/儲蓄目標與提醒
5. 「匯出/匯入」可保存或搬移資料

## LINE Bot 串接草案
未來將新增一個後端（雲端或本機服務）作為 Webhook，並與前端 IndexedDB 同步：
- 同步策略：
  - 前端持續使用 IndexedDB；後端提供 REST API 供上傳/下載資料（含衝突處理以日期與 id 為主）
- LINE 指令建議：
  - 記帳：
    - msg: 「支出 120 餐飲 早餐」→ 解析為 expense，amount=120，category=餐飲，note=早餐
    - 可加日期/幣別/匯率/請款：「支出 120 餐飲 2025-10-12 USD 匯率 32 未請款」
  - 查詢：
    - 「查 本月支出」→ 回傳本月支出總額與前三大分類
    - 「查 2025-10-12」→ 回傳當日交易清單
  - 分類管理：
    - 「+分類 交通」/「-分類 交通」
  - 提醒：
    - 「未請款？」→ 列出未請款清單
    - 「預算？」→ 顯示本月預算進度
- 回覆格式：
  - 精簡文字，必要時附上 quick replies（新增/編輯/標記已請款）

## 心理學與 AI 設計
- 助推（Nudge）：接近預算、未請款、連續紀錄等提醒
- 自動學習：從備註詞彙累積關聯到分類，逐步提升建議準確度
- 情緒/動機：可在表單或 NLP 中標註，洞察區將顯示關聯

## 部署（GitHub + Render）

### 1) 推上 GitHub
```bash
cd /Users/handemo/Desktop/記帳系統
git init
git branch -m main
git add .
git commit -m "feat: ledger app with iOS 26 UI, AI, remote backend"
git remote add origin https://github.com/Miiduoa/math.git
git push -u origin main
```

### 2) Render 新增 Web Service
- Repository: `Miiduoa/math`
- Root Directory: `backend`
- Environment: Node 18+
- Build Command: `npm install`
- Start Command: `npm start`
- Environment Variables:
  - `OPENAI_API_KEY`（必填）
  - 可選：`OPENAI_MODEL=gpt-4o-mini`、`OPENAI_BASE_URL=https://api.openai.com/v1`

佈署完成後，服務會同網域提供前端（index.html、styles.css、app.js、db.js）與 API。

### 3) 前端設定伺服器位址
- 網站「設定 → 同步伺服器位址」填入 Render 網址，例如：`https://YOUR-SERVICE.onrender.com`
- 儲存後即可開始使用（AI 與資料皆走遠端）

## 使用 ChatAnywhere（GPT_API_free）

本專案已支援 OpenAI 兼容協議，可直接接上 ChatAnywhere 的免費/付費 API：

- 設定環境變數（任選其一變數名）：
  - `OPENAI_BASE_URL=https://api.chatanywhere.tech/v1`（中國內地建議）或 `https://api.chatanywhere.org/v1`
  - 或使用 `OPENAI_API_BASE`（相容名稱）：`OPENAI_API_BASE=https://api.chatanywhere.tech/v1`
  - `OPENAI_API_KEY=你的 ChatAnywhere Key`
- 前端也提供「AI 管理」面板（管理者）：
  - 可輸入 `OPENAI_API_KEY` 與 `OPENAI_BASE_URL`，點「儲存並套用」立即生效（重啟後需重新設定或改用環境變數）。
  - 點「自動健檢」可快速檢查 Base/Key、聊天、結構化與 Embedding 是否正常。

參考 ChatAnywhere 文件：https://github.com/chatanywhere/GPT_API_free
