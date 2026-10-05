# math｜Double-entry Personal Finance Ledger

這個 repo 原本是一個功能很多的記帳網站，包含 AI 助理、遠端後端與大量前端程式。現在重做成一個更小、但資料規則更清楚的個人財務帳本。

重點不是聊天輸入，而是：

**每一筆交易都必須能被帳務規則驗證。**

## 核心規則

- 每筆 transaction 必須至少有 2 個 entries
- debit total 必須等於 credit total
- 每個 entry 只能有 debit 或 credit，不可同時有
- account 必須存在
- transaction id 不可重複
- 金額使用整數 minor units，避免浮點誤差

## 帳戶類型

目前支援：

- asset
- liability
- income
- expense
- equity

不同帳戶依 normal side 計算餘額：

- asset / expense：debit - credit
- liability / income / equity：credit - debit

## 快速執行

不需要第三方套件。

```bash
npm test
npm start
```

`npm start` 會讀取 `sample/ledger.json`，輸出：

- account balances
- trial balance
- monthly income
- monthly expense
- monthly net income

## 範例交易

薪資入帳：

```json
{
  "id": "tx-001",
  "date": "2026-10-01",
  "description": "Salary",
  "entries": [
    {"account": "bank", "debit": 5000000, "credit": 0},
    {"account": "salary", "debit": 0, "credit": 5000000}
  ]
}
```

上面的 `5000000` 代表 TWD 50,000.00。

## 為什麼用 minor units

財務系統不適合直接依賴 binary floating point。

例如 `0.1 + 0.2` 在多數程式語言不會精確等於 `0.3`。這個版本用整數儲存最小貨幣單位，所有平衡檢查都在整數上完成。

## 專案結構

```text
src/
  ledger.js
  reports.js
  money.js
cli.js
sample/ledger.json
test/
.github/workflows/test.yml
```

## 限制

- 目前只有單一 base currency
- 沒有銀行同步
- 沒有利息、攤銷、稅務規則
- 沒有正式會計期間結帳流程
- 這是資料一致性與帳務規則實作，不是會計軟體

這個版本刻意拿掉「AI 記帳」包裝，因為對一個財務系統來說，帳務是否平衡比輸入方式更重要。
