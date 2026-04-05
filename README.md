# TradingRobot

## Multi-Asset Auto Trading System (Stock + Crypto Separated)

### Requirements
- Python 3.11+

### Install
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run (Paper Mode)
```bash
python3 orchestrator.py --base-dir . --config config/config.yaml
```

### Run With Signal Emit
```bash
python3 orchestrator.py --base-dir . --config config/config.yaml --emit-signals
```
출력은 한글 요약과 함께 현재 포트폴리오를 포함합니다.

### Telegram Setup
1. Telegram에서 `@BotFather` 열기
2. `/newbot` 실행해서 `bot_token` 발급
3. `@tradingrasbot` 대화창 열고 `/start` 1회 전송
4. 자동 설정 실행 (`chat_id` 자동 탐지 + config 반영)
```bash
python3 setup_telegram.py --bot-token "<YOUR_BOT_TOKEN>" --enable
```
5. 오케스트레이터 실행
```bash
python3 orchestrator.py --base-dir . --config config/config.yaml --emit-signals
```

### Telegram Env Override (Optional)
```bash
export TRADING_TELEGRAM_BOT_TOKEN="<YOUR_BOT_TOKEN>"
export TRADING_TELEGRAM_CHAT_ID="<YOUR_CHAT_ID>"
python3 orchestrator.py --base-dir . --config config/config.yaml --emit-signals
```

### Recovery
```bash
python3 recovery.py --base-dir .
```

### Monitor
```bash
python3 monitor.py --base-dir .
```

### Test
```bash
pytest -q
```

### Notes
- API 없이 `mock` 데이터로 즉시 실행됩니다.
- `state/*.json`에 포지션/평균가/손절가/상태가 저장되고 재시작 시 자동 로드됩니다.
- `logs/decisions.log`, `logs/pnl.log`, `logs/violations.log`가 강제 기록됩니다.
- 생성된 신호는 `logs/signals.log`에 기록되고, `--emit-signals`로 콘솔 출력할 수 있습니다.
- 텔레그램 알림은 신호/시스템/리스크를 전송하며 같은 신호는 1회만 보냅니다.
- ENFORCEMENT 실패 시 신규 진입 차단(Fail-Closed), 기존 포지션 관리만 허용됩니다.
- 매수(enter) 체결 시 포트폴리오에 자동 반영되고, 매도(exit) 체결 시 자동 제거됩니다.
- 포트폴리오 스냅샷은 `state/portfolio.json`에 저장됩니다.

### Raspberry Pi 3 (systemd)
```bash
sudo cp deploy/systemd/trading.service /etc/systemd/system/trading.service
cp deploy/systemd/trading.env.example deploy/systemd/trading.env
nano deploy/systemd/trading.env
sudo systemctl daemon-reload
sudo systemctl enable --now trading
sudo systemctl status trading
```
