#!/usr/bin/env bash
set -euo pipefail

# --- Config (can also be overridden via env when running) ---
DOMAIN="${DOMAIN:-mnogunik.ru}"
WWW_DOMAIN="${WWW_DOMAIN:-www.mnogunik.ru}"
EMAIL="${EMAIL:-}"   # set env EMAIL=you@example.com when running to avoid prompt

# Build email args for certbot
EMAIL_ARGS=()
if [[ -n "$EMAIL" ]]; then
  EMAIL_ARGS=(--agree-tos -m "$EMAIL")
else
  echo ">>> EMAIL not provided. Will register without email (not recommended)."
  EMAIL_ARGS=(--register-unsafely-without-email)
fi

echo ">>> Using domain(s): $DOMAIN, $WWW_DOMAIN"

# 1) Ensure nginx is running
if ! systemctl is-active --quiet nginx; then
  echo ">>> Starting nginx..."
  sudo systemctl start nginx
fi

# 2) Install certbot + nginx plugin via apt
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -y
sudo apt-get install -y certbot python3-certbot-nginx

# 3) Show certbot version
certbot --version

# 4) Test nginx config
sudo nginx -t

# 5) Obtain/Install certificate and enable HTTP->HTTPS redirect
sudo certbot --nginx -d "$DOMAIN" -d "$WWW_DOMAIN" --redirect -n "${EMAIL_ARGS[@]}"

# 6) Show renewal timer status (apt package sets this up automatically)
systemctl status certbot.timer --no-pager || true

# 7) Quick check
echo ">>> Checking HTTPS:"
curl -I --max-time 10 "https://$DOMAIN" || true
