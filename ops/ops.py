from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CUR = Path(__file__).resolve().parent
if sys.path and Path(sys.path[0]).resolve() == CUR:
    sys.path.pop(0)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import load_config
from engines.drift import evaluate_drift
from ops.monitor import monitor_status
from state.store import (
    load_capital_events,
    load_json_state,
    load_system_state,
    load_trades,
    save_capital_events,
    save_json_state,
    save_system_state,
)


def cmd_status(base_dir: Path) -> dict[str, Any]:
    return monitor_status(base_dir)


def cmd_watchlist(base_dir: Path) -> dict[str, Any]:
    return {
        "stock_active": load_json_state(base_dir, "watchlist_stock_active.json", {"symbols": []}),
        "crypto_active": load_json_state(base_dir, "watchlist_crypto_active.json", {"symbols": []}),
        "stock_auto": load_json_state(base_dir, "watchlist_stock_auto.json", {"symbols": []}),
        "crypto_auto": load_json_state(base_dir, "watchlist_crypto_auto.json", {"symbols": []}),
    }


def cmd_unlock_safe_mode(base_dir: Path) -> dict[str, Any]:
    s = load_system_state(base_dir)
    s["safe_mode"] = False
    s["violation_streak"] = 0
    save_system_state(base_dir, s)
    return {"ok": True, "safe_mode": s["safe_mode"], "violation_streak": s["violation_streak"]}


def cmd_force_drift(base_dir: Path, config: dict[str, Any]) -> dict[str, Any]:
    s = load_system_state(base_dir)
    t = load_trades(base_dir)
    now = datetime.now(timezone.utc)
    drift = evaluate_drift(now, t, config, s)
    s["drift_last_checked"] = now.isoformat()
    s["drift_last_warning"] = drift.get("warning", "unknown")
    s["drift_last_trade_count"] = len(t)
    save_system_state(base_dir, s)
    return drift


def cmd_clear_idempotency(base_dir: Path) -> dict[str, Any]:
    save_json_state(base_dir, "idempotency.json", {"seen": []})
    save_json_state(base_dir, "alert_idempotency.json", {"seen": []})
    return {"ok": True}


def cmd_portfolio(base_dir: Path) -> dict[str, Any]:
    return load_json_state(base_dir, "portfolio.json", {})


def cmd_apply_watchlist(base_dir: Path) -> dict[str, Any]:
    stock_auto = load_json_state(base_dir, "watchlist_stock_auto.json", {"symbols": []})
    crypto_auto = load_json_state(base_dir, "watchlist_crypto_auto.json", {"symbols": []})
    save_json_state(
        base_dir,
        "watchlist_stock_active.json",
        {"timestamp": datetime.now(timezone.utc).isoformat(), "symbols": list(stock_auto.get("symbols", []))},
    )
    save_json_state(
        base_dir,
        "watchlist_crypto_active.json",
        {"timestamp": datetime.now(timezone.utc).isoformat(), "symbols": list(crypto_auto.get("symbols", []))},
    )
    return {"ok": True, "stock": stock_auto.get("symbols", []), "crypto": crypto_auto.get("symbols", [])}


def cmd_add_capital_event(base_dir: Path, event_type: str, amount: float, note: str) -> dict[str, Any]:
    rows = load_capital_events(base_dir)
    row = {
        "timestamp": datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat(),
        "type": event_type,
        "amount": round(amount, 4),
        "note": note,
    }
    rows.append(row)
    save_capital_events(base_dir, rows)
    return {"ok": True, "event": row, "count": len(rows)}


def main() -> None:
    parser = argparse.ArgumentParser(description="TradingRobot ops commands")
    parser.add_argument(
        "command",
        choices=[
            "status",
            "watchlist",
            "unlock-safe-mode",
            "force-drift",
            "clear-idempotency",
            "portfolio",
            "apply-watchlist",
            "add-capital-event",
        ],
    )
    parser.add_argument("--base-dir", default=".")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--event-type", choices=["deposit", "withdraw", "manual_adjustment"], default="manual_adjustment")
    parser.add_argument("--amount", type=float, default=0.0)
    parser.add_argument("--note", default="")
    args = parser.parse_args()

    base = Path(args.base_dir).resolve()
    cfg = load_config(base, args.config)

    if args.command == "status":
        out = cmd_status(base)
    elif args.command == "watchlist":
        out = cmd_watchlist(base)
    elif args.command == "unlock-safe-mode":
        out = cmd_unlock_safe_mode(base)
    elif args.command == "force-drift":
        out = cmd_force_drift(base, cfg)
    elif args.command == "clear-idempotency":
        out = cmd_clear_idempotency(base)
    elif args.command == "apply-watchlist":
        out = cmd_apply_watchlist(base)
    elif args.command == "add-capital-event":
        out = cmd_add_capital_event(base, args.event_type, args.amount, args.note)
    else:
        out = cmd_portfolio(base)

    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
