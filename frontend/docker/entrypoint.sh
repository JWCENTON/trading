#!/bin/sh
set -eu

: "${VITE_API_BASE_URL:=/api}"
: "${VITE_API_BASE_URL_LIVE:=$VITE_API_BASE_URL}"
: "${VITE_API_BASE_URL_PAPER:=$VITE_API_BASE_URL}"
: "${VITE_QUOTE_ASSET:=USDC}"

cat >/usr/share/nginx/html/env.js <<EOF
window.__ENV__ = window.__ENV__ || {};
window.__ENV__.VITE_API_BASE_URL = "${VITE_API_BASE_URL}";
window.__ENV__.VITE_API_BASE_URL_LIVE = "${VITE_API_BASE_URL_LIVE}";
window.__ENV__.VITE_API_BASE_URL_PAPER = "${VITE_API_BASE_URL_PAPER}";
window.__ENV__.VITE_QUOTE_ASSET = "${VITE_QUOTE_ASSET}";
EOF

exec nginx -g 'daemon off;'