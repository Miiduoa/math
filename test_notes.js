#!/usr/bin/env node
// 測試記事功能
const http = require('http');

const BASE_URL = process.env.BASE_URL || 'http://localhost:8787';
const TEST_USER = 'test-user-notes';

async function testNotesAPI() {
  console.log('🧪 測試記事 API 功能...\n');

  // 測試新增記事
  console.log('1. 測試新增記事...');
  const addPayload = {
    title: '測試記事標題',
    content: '這是一個測試記事內容，用來驗證記事功能是否正常運作。',
    tags: ['測試', '記事', 'API'],
    emoji: '📝',
    color: 'blue',
    pinned: false,
    archived: false
  };

  try {
    const addResponse = await fetch(`${BASE_URL}/api/notes`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(addPayload)
    });
    
    if (!addResponse.ok) {
      throw new Error(`新增失敗: ${addResponse.status}`);
    }
    
    const addResult = await addResponse.json();
    console.log('✅ 新增成功:', addResult.note?.id);
    const noteId = addResult.note?.id;

    // 測試取得記事清單
    console.log('\n2. 測試取得記事清單...');
    const listResponse = await fetch(`${BASE_URL}/api/notes`);
    if (!listResponse.ok) {
      throw new Error(`取得清單失敗: ${listResponse.status}`);
    }
    
    const notes = await listResponse.json();
    console.log(`✅ 取得 ${notes.length} 筆記事`);
    
    if (notes.length > 0) {
      const latestNote = notes[0];
      console.log('最新記事:', {
        id: latestNote.id,
        title: latestNote.title,
        content: latestNote.content.slice(0, 50) + '...',
        tags: latestNote.tags,
        emoji: latestNote.emoji,
        color: latestNote.color
      });
    }

    // 測試更新記事
    if (noteId) {
      console.log('\n3. 測試更新記事...');
      const updatePayload = {
        title: '更新後的記事標題',
        content: '這是更新後的記事內容',
        tags: ['更新', '測試'],
        emoji: '✏️',
        color: 'green'
      };

      const updateResponse = await fetch(`${BASE_URL}/api/notes/${encodeURIComponent(noteId)}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updatePayload)
      });

      if (!updateResponse.ok) {
        throw new Error(`更新失敗: ${updateResponse.status}`);
      }

      const updateResult = await updateResponse.json();
      console.log('✅ 更新成功:', updateResult.note?.title);

      // 測試刪除記事
      console.log('\n4. 測試刪除記事...');
      const deleteResponse = await fetch(`${BASE_URL}/api/notes/${encodeURIComponent(noteId)}`, {
        method: 'DELETE'
      });

      if (!deleteResponse.ok) {
        throw new Error(`刪除失敗: ${deleteResponse.status}`);
      }

      console.log('✅ 刪除成功');
    }

    console.log('\n🎉 所有記事功能測試通過！');

  } catch (error) {
    console.error('❌ 測試失敗:', error.message);
    process.exit(1);
  }
}

// 執行測試
testNotesAPI().catch(console.error);
