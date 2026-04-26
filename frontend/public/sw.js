const CACHE_NAME = "waltrade-bot-v1";

self.addEventListener("install", () => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  if (event.request.method !== "GET") return;
  if (url.pathname.startsWith("/api/")) return;
  if (url.pathname === "/env.js") return;

  event.respondWith(
    fetch(event.request).catch(() => caches.match(event.request))
  );
});
