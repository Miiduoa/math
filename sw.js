/* Simple offline-first Service Worker with iOS-like smoothness focus */
const CACHE_VERSION = 'app-cache-v20251021-1';
const CORE_ASSETS = [
  '/',
  '/index.html',
  '/styles.css?v=ios-glass-20251012-2',
  '/app.js?v=20251021-1',
  '/db.js?v=20251021-1',
  '/flex-glass.svg',
  '/manifest.webmanifest'
];

self.addEventListener('install', (event) => {
  self.skipWaiting();
  event.waitUntil(
    caches.open(CACHE_VERSION).then((cache) => cache.addAll(CORE_ASSETS)).catch(() => {})
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    (async () => {
      const names = await caches.keys();
      await Promise.all(names.filter((n) => n !== CACHE_VERSION).map((n) => caches.delete(n)));
      await self.clients.claim();
    })()
  );
});

// Helper: match with ignoreSearch fallback
async function matchWithFallback(request){
  let res = await caches.match(request);
  if(!res){
    try{ res = await caches.match(new Request(new URL(request.url).pathname, {method:'GET'})); }catch(_){ }
  }
  return res || null;
}

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if(req.method !== 'GET') return;

  const url = new URL(req.url);
  const isHTML = req.mode === 'navigate' || (req.headers.get('accept') || '').includes('text/html');
  const isAsset = ['style','script','image','font','manifest'].includes(req.destination) || /\.(css|js|svg|png|jpg|webmanifest)(\?.*)?$/i.test(url.pathname);

  // Network-first for HTML navigations (fresh content, offline fallback)
  if(isHTML){
    event.respondWith(
      (async()=>{
        try{
          const fresh = await fetch(req);
          const cache = await caches.open(CACHE_VERSION);
          cache.put(req, fresh.clone());
          return fresh;
        }catch(_){
          const cached = await matchWithFallback(req);
          return cached || caches.match('/index.html');
        }
      })()
    );
    return;
  }

  // Stale-while-revalidate for static assets (smoothness)
  if(isAsset){
    event.respondWith(
      (async()=>{
        const cache = await caches.open(CACHE_VERSION);
        const cached = await matchWithFallback(req);
        const fetchPromise = fetch(req).then((resp)=>{ try{ cache.put(req, resp.clone()); }catch(_){ } return resp; }).catch(()=>null);
        return cached || (await fetchPromise) || new Response('', { status: 504 });
      })()
    );
  }
});

