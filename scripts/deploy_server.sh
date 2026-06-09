#!/usr/bin/env bash
set -euo pipefail

APP_NAME="${APP_NAME:-social-scraper}"
APP_DIR="${APP_DIR:-/opt/social-scraper}"
DATA_DIR="${DATA_DIR:-/opt/social-scraper-data}"
IMAGE_NAME="${IMAGE_NAME:-social-scraper}"
ENV_FILE="${ENV_FILE:-$APP_DIR/.env}"

mkdir -p "$DATA_DIR"
touch "$DATA_DIR/auth_settings.json"
touch "$DATA_DIR/sheet_tabs_cache.json"
touch "$DATA_DIR/sheet_data_cache.json"
touch "$DATA_DIR/dashboard_cache.json"

cd "$APP_DIR"

EXTRA_MOUNTS=()

if [ -f "$APP_DIR/credential.json" ]; then
  EXTRA_MOUNTS+=("-v" "$APP_DIR/credential.json:/app/credential.json:ro")
fi

if [ -f "$APP_DIR/tiktok_cookies.json" ]; then
  EXTRA_MOUNTS+=("-v" "$APP_DIR/tiktok_cookies.json:/app/tiktok_cookies.json:ro")
fi

if [ -d "$APP_DIR/tiktok-profile" ]; then
  EXTRA_MOUNTS+=("-v" "$APP_DIR/tiktok-profile:/app/tiktok-profile")
fi

docker build -t "$IMAGE_NAME" .
docker rm -f "$APP_NAME" 2>/dev/null || true

docker run -d --name "$APP_NAME" \
  --restart unless-stopped \
  --env-file "$ENV_FILE" \
  -v "$DATA_DIR/auth_settings.json:/app/auth_settings.json" \
  -v "$DATA_DIR/sheet_tabs_cache.json:/app/sheet_tabs_cache.json" \
  -v "$DATA_DIR/sheet_data_cache.json:/app/sheet_data_cache.json" \
  -v "$DATA_DIR/dashboard_cache.json:/app/dashboard_cache.json" \
  "${EXTRA_MOUNTS[@]}" \
  -p 8000:8000 \
  "$IMAGE_NAME"

docker logs --tail 80 "$APP_NAME"
