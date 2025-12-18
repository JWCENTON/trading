#!/bin/sh
set -eu

: "${VITE_API_BASE_URL:=http://localhost:8001}"

cat >/usr/share/nginx/html/env.js <<EOF
window.__ENV__ = window.__ENV__ || {};
window.__ENV__.VITE_API_BASE_URL = "${VITE_API_BASE_URL}";
EOF

exec nginx -g 'daemon off;'