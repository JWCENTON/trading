// Runtime env injected by container entrypoint.
// Do not commit secrets here.
window.__ENV__ = window.__ENV__ || {};
window.__ENV__.VITE_API_BASE_URL = "http://localhost:8001";