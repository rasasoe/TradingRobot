#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

python3 -m venv .venv
# shellcheck disable=SC1091
source .venv/bin/activate
pip install -r requirements.txt

mkdir -p deploy/systemd logs

read -r -p "Telegram bot token 입력: " BOT_TOKEN
read -r -p "Telegram chat_id 입력(모르면 엔터): " CHAT_ID

if [[ -z "${BOT_TOKEN}" ]]; then
  echo "bot token is required"
  exit 1
fi

if [[ -n "${CHAT_ID}" ]]; then
  python3 setup_telegram.py --bot-token "$BOT_TOKEN" --chat-id "$CHAT_ID" --enable
else
  python3 setup_telegram.py --bot-token "$BOT_TOKEN" --enable
fi

cat > deploy/systemd/trading.env <<ENV
TRADING_TELEGRAM_BOT_TOKEN=${BOT_TOKEN}
TRADING_TELEGRAM_CHAT_ID=$(python3 - <<'PY'
from pathlib import Path
from config.settings import load_config
cfg = load_config(Path('.').resolve(), 'config/config.yaml')
print(cfg.get('notifications', {}).get('telegram', {}).get('chat_id', ''))
PY
)
ENV

if [[ -w /etc/systemd/system ]]; then
  cp deploy/systemd/trading.service /etc/systemd/system/trading.service
else
  sudo cp deploy/systemd/trading.service /etc/systemd/system/trading.service
fi

sudo systemctl daemon-reload
sudo systemctl enable --now trading
sudo systemctl status trading --no-pager

python3 orchestrator.py --base-dir . --config config/config.yaml --emit-signals
