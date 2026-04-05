from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=True) + "\n")


def ensure_state_files(base_dir: Path) -> None:
    defaults = {
        "positions.json": {},
        "portfolio.json": {"timestamp": None, "stock": [], "crypto": [], "total_positions": 0},
        "idempotency.json": {"seen": []},
        "alert_idempotency.json": {"seen": []},
        "system_state.json": {
            "violation_streak": 0,
            "safe_mode": False,
            "last_heartbeat": None,
            "r_multiplier": 1.0,
            "disabled_strategies": [],
        },
        "trades.json": [],
        "pending_orders.json": [],
        "capital_events.json": [],
    }
    for name, default in defaults.items():
        path = base_dir / "state" / name
        if not path.exists():
            _save_json(path, default)


def load_positions(base_dir: Path) -> dict[str, Any]:
    return _load_json(base_dir / "state" / "positions.json", {})


def save_positions(base_dir: Path, positions: dict[str, Any]) -> None:
    _save_json(base_dir / "state" / "positions.json", positions)


def load_portfolio(base_dir: Path) -> dict[str, Any]:
    return _load_json(
        base_dir / "state" / "portfolio.json",
        {"timestamp": None, "stock": [], "crypto": [], "total_positions": 0},
    )


def save_portfolio(base_dir: Path, portfolio: dict[str, Any]) -> None:
    _save_json(base_dir / "state" / "portfolio.json", portfolio)


def load_idempotency(base_dir: Path) -> dict[str, Any]:
    return _load_json(base_dir / "state" / "idempotency.json", {"seen": []})


def save_idempotency(base_dir: Path, idempotency: dict[str, Any]) -> None:
    _save_json(base_dir / "state" / "idempotency.json", idempotency)


def load_alert_idempotency(base_dir: Path) -> dict[str, Any]:
    return _load_json(base_dir / "state" / "alert_idempotency.json", {"seen": []})


def save_alert_idempotency(base_dir: Path, idempotency: dict[str, Any]) -> None:
    _save_json(base_dir / "state" / "alert_idempotency.json", idempotency)


def load_system_state(base_dir: Path) -> dict[str, Any]:
    return _load_json(
        base_dir / "state" / "system_state.json",
        {
            "violation_streak": 0,
            "safe_mode": False,
            "last_heartbeat": None,
            "r_multiplier": 1.0,
            "disabled_strategies": [],
        },
    )


def save_system_state(base_dir: Path, state: dict[str, Any]) -> None:
    _save_json(base_dir / "state" / "system_state.json", state)


def load_trades(base_dir: Path) -> list[dict[str, Any]]:
    return _load_json(base_dir / "state" / "trades.json", [])


def save_trades(base_dir: Path, trades: list[dict[str, Any]]) -> None:
    _save_json(base_dir / "state" / "trades.json", trades)


def load_pending_orders(base_dir: Path) -> list[dict[str, Any]]:
    return _load_json(base_dir / "state" / "pending_orders.json", [])


def save_pending_orders(base_dir: Path, orders: list[dict[str, Any]]) -> None:
    _save_json(base_dir / "state" / "pending_orders.json", orders)


def load_capital_events(base_dir: Path) -> list[dict[str, Any]]:
    return _load_json(base_dir / "state" / "capital_events.json", [])
