from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        # Corrupted state must not crash the service loop. Keep a backup and self-heal.
        try:
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            backup = path.with_suffix(path.suffix + f".corrupt.{ts}")
            path.replace(backup)
        except OSError:
            pass
        _save_json(path, default)
        return default


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)
    tmp.replace(path)


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=True) + "\n")


def ensure_state_files(base_dir: Path) -> None:
    defaults: dict[str, Any] = {
        "positions.json": {},
        "portfolio.json": {"timestamp": None, "stock": [], "crypto": [], "total_positions": 0},
        "idempotency.json": {"seen": []},
        "alert_idempotency.json": {"seen": []},
        "system_state.json": {
            "violation_streak": 0,
            "safe_mode": False,
            "last_heartbeat": None,
            "fast_heartbeat": None,
            "r_multiplier": 1.0,
            "disabled_strategies": [],
            "pnl_log_fail_streak": 0,
            "drift_last_checked": None,
            "drift_last_warning": "unknown",
            "drift_last_trade_count": 0,
            "cash": None,
            "peak_equity": None,
            "selector_stock_last_run": None,
            "selector_crypto_last_run": None,
        },
        "trades.json": [],
        "pending_orders.json": [],
        "capital_events.json": [],
        "watchlist_stock_auto.json": {"timestamp": None, "symbols": []},
        "watchlist_crypto_auto.json": {"timestamp": None, "symbols": []},
        "watchlist_stock_active.json": {"timestamp": None, "symbols": []},
        "watchlist_crypto_active.json": {"timestamp": None, "symbols": []},
    }
    for filename, default in defaults.items():
        path = base_dir / "state" / filename
        if not path.exists():
            _save_json(path, default)


def load_json_state(base_dir: Path, filename: str, default: Any) -> Any:
    return _load_json(base_dir / "state" / filename, default)


def save_json_state(base_dir: Path, filename: str, data: Any) -> None:
    _save_json(base_dir / "state" / filename, data)


def load_positions(base_dir: Path) -> dict[str, Any]:
    return load_json_state(base_dir, "positions.json", {})


def save_positions(base_dir: Path, positions: dict[str, Any]) -> None:
    save_json_state(base_dir, "positions.json", positions)


def load_portfolio(base_dir: Path) -> dict[str, Any]:
    return load_json_state(base_dir, "portfolio.json", {"timestamp": None, "stock": [], "crypto": [], "total_positions": 0})


def save_portfolio(base_dir: Path, portfolio: dict[str, Any]) -> None:
    save_json_state(base_dir, "portfolio.json", portfolio)


def load_idempotency(base_dir: Path) -> dict[str, Any]:
    return load_json_state(base_dir, "idempotency.json", {"seen": []})


def save_idempotency(base_dir: Path, data: dict[str, Any]) -> None:
    save_json_state(base_dir, "idempotency.json", data)


def load_alert_idempotency(base_dir: Path) -> dict[str, Any]:
    return load_json_state(base_dir, "alert_idempotency.json", {"seen": []})


def save_alert_idempotency(base_dir: Path, data: dict[str, Any]) -> None:
    save_json_state(base_dir, "alert_idempotency.json", data)


def load_system_state(base_dir: Path) -> dict[str, Any]:
    return load_json_state(base_dir, "system_state.json", {})


def save_system_state(base_dir: Path, data: dict[str, Any]) -> None:
    save_json_state(base_dir, "system_state.json", data)


def load_trades(base_dir: Path) -> list[dict[str, Any]]:
    return load_json_state(base_dir, "trades.json", [])


def save_trades(base_dir: Path, trades: list[dict[str, Any]]) -> None:
    save_json_state(base_dir, "trades.json", trades)


def load_pending_orders(base_dir: Path) -> list[dict[str, Any]]:
    return load_json_state(base_dir, "pending_orders.json", [])


def save_pending_orders(base_dir: Path, rows: list[dict[str, Any]]) -> None:
    save_json_state(base_dir, "pending_orders.json", rows)


def load_capital_events(base_dir: Path) -> list[dict[str, Any]]:
    return load_json_state(base_dir, "capital_events.json", [])


def save_capital_events(base_dir: Path, rows: list[dict[str, Any]]) -> None:
    save_json_state(base_dir, "capital_events.json", rows)
