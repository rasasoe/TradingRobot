# TradingRobot (Raspberry Pi 3)

Pi 3 운영 기준(저빈도/저I/O) 멀티자산 감시/알림/자동매매 시스템입니다.

## 1) 구조
```text
StockPlatform/
├── config/
│   ├── config.yaml
│   ├── settings.py
│   └── secrets.example.yaml
├── data/
│   ├── providers/
│   ├── cache/
│   └── mock/
├── engines/
│   ├── stock_engine.py
│   ├── crypto_engine.py
│   ├── selector_stock.py
│   ├── selector_crypto.py
│   ├── session_guard.py
│   └── drift.py
├── execution/
│   ├── executor.py
│   ├── order_router.py
│   └── reconciliation.py
├── risk/
│   ├── risk_manager.py
│   ├── enforcement.py
│   └── safe_mode.py
├── notifications/
│   ├── telegram_notifier.py
│   ├── console_notifier.py
│   └── router.py
├── ops/
│   ├── monitor.py
│   ├── recovery.py
│   ├── log_rotate.py
│   └── ops.py
├── state/
├── logs/
├── tests/
├── orchestrator.py
├── orchestrator_fast.py
├── fast_monitor.py
├── monitor.py
├── recovery.py
├── requirements.txt
└── deploy/systemd/
    ├── trading.service
    └── trading-fast.service
```

## 2) 모드
- `paper`: 키 없이 즉시 실행, API 실패 시 mock fallback.
- `live_small`: 소액 운용, fail-closed 동일 강제.

## 3) 핵심 동작
- 메인 루프 `60초`: 데이터/신규진입/리스크/주문/성과/로그/알림.
- 빠른 루프 `15초`: 기존 포지션 stop 감시 + 메인 heartbeat stale 감지.
- `stock/crypto` 엔진 분리.
- idempotency key: `asset + side + candle_timestamp + strategy_name + action`.
- fail-closed:
  - `TIME SYNC` 불일치
  - `PnL` 기록 실패(1회 재시도 후 연속 실패만 violation)
  - drift 상태 미확인
  - capital event candle
- violation 3회 연속 시 safe mode.
- watchlist selector:
  - stock 24시간
  - crypto 4시간
  - static/auto/active 분리 운영

## 4) 설치
```bash
cd /Users/rasasoe/workspace/StockPlatform
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 5) 즉시 실행 (Paper)
```bash
cd /Users/rasasoe/workspace/StockPlatform
source .venv/bin/activate
python3 orchestrator.py --base-dir . --config config/config.yaml --emit-signals
python3 orchestrator_fast.py --base-dir . --config config/config.yaml
```

## 6) 텔레그램 설정
```bash
cd /Users/rasasoe/workspace/StockPlatform
source .venv/bin/activate
export TRADING_TELEGRAM_BOT_TOKEN='YOUR_TOKEN'
export TRADING_TELEGRAM_CHAT_ID='YOUR_CHAT_ID'
python3 setup_telegram.py --enable
```
- 토큰은 `config/secrets.yaml` 또는 환경변수 사용.
- 토큰이 없어도 paper 모드 실행 가능.

## 7) Ops 명령
```bash
python3 ops/ops.py status --base-dir .
python3 ops/ops.py watchlist --base-dir .
python3 ops/ops.py portfolio --base-dir .
python3 ops/ops.py force-drift --base-dir . --config config/config.yaml
python3 ops/ops.py unlock-safe-mode --base-dir .
python3 ops/ops.py clear-idempotency --base-dir .
python3 ops/ops.py apply-watchlist --base-dir .
python3 ops/ops.py add-capital-event --base-dir . --event-type deposit --amount 1000 --note "manual topup"
```

## 8) 모니터/복구
```bash
python3 monitor.py --base-dir .
python3 recovery.py --base-dir . --mode paper
```

## 9) systemd (Pi3)
```bash
sudo cp deploy/systemd/trading.service /etc/systemd/system/trading.service
sudo cp deploy/systemd/trading-fast.service /etc/systemd/system/trading-fast.service
cp deploy/systemd/trading.env.example deploy/systemd/trading.env
sudo systemctl daemon-reload
sudo systemctl enable --now trading
sudo systemctl enable --now trading-fast
sudo systemctl status trading --no-pager
sudo systemctl status trading-fast --no-pager
```

## 10) 로그/상태 파일
- 로그:
  - `logs/decisions.log`
  - `logs/pnl.log`
  - `logs/performance.log` (`return_pct` 포함)
  - `logs/violations.log`
- 상태:
  - `state/positions.json`
  - `state/portfolio.json`
  - `state/system_state.json`
  - `state/idempotency.json`
  - `state/alert_idempotency.json`
  - `state/watchlist_*`
  - `state/capital_events.json`

## 11) 테스트
```bash
cd /Users/rasasoe/workspace/StockPlatform
source .venv/bin/activate
pytest -q
```

포함 테스트:
- orchestrator 1회 실행
- fail-closed 차단
- idempotency 중복 방지
- watchlist selector 생성
- recovery state load
