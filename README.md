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
- ENFORCEMENT 실패 시 신규 진입 차단(Fail-Closed), 기존 포지션 관리만 허용됩니다.
