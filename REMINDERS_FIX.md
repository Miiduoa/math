# 提醒事項同步問題修復

## 問題描述
LINE 機器人上的提醒事項資料在網頁上看不到。

## 根本原因
系統在資料庫模式（database mode）下缺少 `reminders` 表格的定義和對應的 CRUD API 實作。原本只在檔案模式（file-based mode）下支援提醒事項功能。

## 修復內容

### 1. 資料庫 Schema 更新 (`backend/db.js`)
- 新增 `reminders` 表格定義
- 包含所有必要欄位：id, user_id, title, due_at, repeat, weekdays, month_day, priority, tags, note, done, created_at, updated_at
- 新增使用者索引以提升查詢效能

### 2. 資料庫 CRUD 方法 (`backend/db.js`)
- `getReminders(userId)` - 取得使用者的提醒清單
- `addReminder(userId, payload)` - 新增提醒
- `updateReminder(userId, id, patch)` - 更新提醒
- `getReminderById(userId, id)` - 取得特定提醒
- `deleteReminder(userId, id)` - 刪除提醒

### 3. API 端點更新 (`backend/server.js`)
- 更新 `/api/reminders` GET 端點支援資料庫模式
- 更新 `/api/reminders` POST 端點支援資料庫模式
- 更新 `/api/reminders/{id}` PUT 端點支援資料庫模式
- 更新 `/api/reminders/{id}` DELETE 端點支援資料庫模式

### 4. LINE 機器人整合更新 (`backend/server.js`)
- 更新提醒清單查詢支援資料庫模式
- 更新快速新增提醒支援資料庫模式
- 更新提醒流程確認步驟支援資料庫模式

## 測試方法

### 1. 啟動伺服器
```bash
cd backend
npm start
```

### 2. 執行測試腳本
```bash
node test_reminders.js
```

### 3. 手動測試
1. 在 LINE 機器人中輸入「提醒：測試提醒 2025-01-15」
2. 在網頁版中切換到「提醒」分頁
3. 確認提醒事項已同步顯示

## 資料庫遷移
如果已有現有資料庫，系統會自動建立 `reminders` 表格。無需手動遷移。

## 向後相容性
- 檔案模式仍然完全支援
- 資料庫模式和檔案模式可以無縫切換
- 現有資料不會遺失

## 注意事項
- 確保 `DATABASE_URL` 環境變數已正確設定
- 提醒事項會根據使用者 ID 進行隔離
- 所有時間戳記使用 ISO 8601 格式
