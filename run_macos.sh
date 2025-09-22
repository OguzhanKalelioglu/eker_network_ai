#!/usr/bin/env bash
set -euo pipefail

# macOS için FastAPI uygulamasını ve Cloudflare Tunnel'ı birlikte başlatır

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# .env varsa yükle
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

echo "==> Python sanal ortamı hazırlanıyor"
if [ ! -d venv ]; then
  python3 -m venv venv
fi
# shellcheck disable=SC1091
source venv/bin/activate

echo "==> Bağımlılıklar yükleniyor"
if [ -f requirements.txt ]; then
  pip install --upgrade pip >/dev/null
  pip install -r requirements.txt
else
  pip install --upgrade pip >/dev/null
  pip install fastapi uvicorn google-api-python-client google-auth-httplib2 google-auth google-auth-oauthlib httpx
fi

echo "==> Cloudflare Tunnel (cloudflared) kontrol ediliyor"
if ! command -v cloudflared >/dev/null 2>&1; then
  if command -v brew >/dev/null 2>&1; then
    echo "Homebrew ile cloudflared kuruluyor..."
    brew install cloudflared
  else
    echo "HATA: Homebrew bulunamadı. Lütfen cloudflared'ı kurun: https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/"
    exit 1
  fi
fi

LOG_DIR="$DIR/.run_logs"
mkdir -p "$LOG_DIR"

echo "==> Uvicorn başlatılıyor (log: $LOG_DIR/uvicorn.log)"
"$DIR/venv/bin/uvicorn" main:app --host 0.0.0.0 --port 8000 --reload > "$LOG_DIR/uvicorn.log" 2>&1 &
UVICORN_PID=$!

echo "==> Cloudflare Tunnel başlatılıyor (log: $LOG_DIR/cloudflared.log)"
# 'eker-network-ai' tünelini yereldeki 8000 portuna yönlendirerek çalıştır
cloudflared tunnel --no-autoupdate run --url http://localhost:8000 eker-network-ai > "$LOG_DIR/cloudflared.log" 2>&1 &
TUNNEL_PID=$!

# Canlı log akışı
LOG_UV="$LOG_DIR/uvicorn.log"
LOG_TUNNEL="$LOG_DIR/cloudflared.log"
tail -n +1 -f "$LOG_UV" | sed -e 's/^/[uvicorn] /' &
TAIL_UV_PID=$!
tail -n +1 -f "$LOG_TUNNEL" | sed -e 's/^/[tunnel]  /' &
TAIL_TUNNEL_PID=$!

# Çıkışta süreçleri kapat
cleanup() {
  echo
  echo "==> Kapatılıyor..."
  kill "$UVICORN_PID" >/dev/null 2>&1 || true
  kill "$TUNNEL_PID" >/dev/null 2>&1 || true
  kill "$TAIL_UV_PID" >/dev/null 2>&1 || true
  kill "$TAIL_TUNNEL_PID" >/dev/null 2>&1 || true
}
trap cleanup INT TERM

echo ""
echo "==> Uygulama hazır"
PUBLIC_URL="https://network-ai.eker.com"
echo "Local:   http://localhost:8000"
echo "Health:  http://localhost:8000/health"
echo "Public:  $PUBLIC_URL"
echo "Push:    $PUBLIC_URL/pubsub/gmail"
echo "Loglar:  $LOG_DIR"
echo "Çıkmak için CTRL+C"

wait "$UVICORN_PID" "$TUNNEL_PID"


