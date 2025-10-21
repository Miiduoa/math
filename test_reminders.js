#!/usr/bin/env node

// 簡單的測試腳本來驗證 reminders API 是否正常工作
const http = require('http');

const BASE_URL = process.env.BASE_URL || 'http://localhost:8787';

async function testRemindersAPI() {
  console.log('🧪 測試提醒事項 API...\n');

  // 測試 GET /api/reminders
  console.log('1. 測試取得提醒清單...');
  try {
    const getResponse = await fetch(`${BASE_URL}/api/reminders`);
    const reminders = await getResponse.json();
    console.log(`✅ GET /api/reminders: ${reminders.length} 筆提醒`);
  } catch (error) {
    console.log(`❌ GET /api/reminders 失敗: ${error.message}`);
  }

  // 測試 POST /api/reminders
  console.log('\n2. 測試新增提醒...');
  try {
    const newReminder = {
      title: '測試提醒事項',
      dueAt: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(), // 明天
      priority: 'medium',
      note: '這是一個測試提醒'
    };

    const postResponse = await fetch(`${BASE_URL}/api/reminders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(newReminder)
    });

    const result = await postResponse.json();
    if (result.ok && result.reminder) {
      console.log(`✅ POST /api/reminders: 新增成功，ID: ${result.reminder.id}`);
      
      // 測試 PUT /api/reminders/{id}
      console.log('\n3. 測試更新提醒...');
      const updateResponse = await fetch(`${BASE_URL}/api/reminders/${result.reminder.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ done: true })
      });

      const updateResult = await updateResponse.json();
      if (updateResult.ok) {
        console.log(`✅ PUT /api/reminders/{id}: 更新成功`);
      } else {
        console.log(`❌ PUT /api/reminders/{id} 失敗`);
      }

      // 測試 DELETE /api/reminders/{id}
      console.log('\n4. 測試刪除提醒...');
      const deleteResponse = await fetch(`${BASE_URL}/api/reminders/${result.reminder.id}`, {
        method: 'DELETE'
      });

      const deleteResult = await deleteResponse.json();
      if (deleteResult.ok) {
        console.log(`✅ DELETE /api/reminders/{id}: 刪除成功`);
      } else {
        console.log(`❌ DELETE /api/reminders/{id} 失敗`);
      }
    } else {
      console.log(`❌ POST /api/reminders 失敗: ${JSON.stringify(result)}`);
    }
  } catch (error) {
    console.log(`❌ POST /api/reminders 失敗: ${error.message}`);
  }

  console.log('\n🎉 測試完成！');
}

// 檢查是否在 Node.js 環境中
if (typeof fetch === 'undefined') {
  console.log('⚠️  需要 Node.js 18+ 或安裝 node-fetch');
  console.log('請執行: npm install node-fetch');
  process.exit(1);
}

testRemindersAPI().catch(console.error);
