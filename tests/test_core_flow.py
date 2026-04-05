from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import orchestrator
from execution.executor import execute_orders
from ops.recovery import recover
from state.store import (
    ensure_state_files,
    load_idempotency,
    load_json_state,
    load_pending_orders,
    load_positions,
    load_system_state,
    load_trades,
    save_json_state,
    save_pending_orders,
    save_system_state,
)


def _prepare_base(tmp_path: Path) -> Path:
    for folder in ["config", "logs", "state", "data", "data/cache"]:
        (tmp_path / folder).mkdir(parents=True, exist_ok=True)
    cfg = yaml.safe_load((ROOT / "config" / "config.yaml").read_text(encoding="utf-8"))
    cfg.setdefault("data", {})["source"] = "mock"
    cfg.setdefault("notifications", {}).setdefault("telegram", {})["enabled"] = False
    cfg.setdefault("notifications", {}).setdefault("console", {})["enabled"] = False
    (tmp_path / "config" / "config.yaml").write_text(yaml.safe_dump(cfg, allow_unicode=False, sort_keys=False), encoding="utf-8")
    ensure_state_files(tmp_path)
    return tmp_path


def test_orchestrator_one_run(tmp_path: Path) -> None:
    base = _prepare_base(tmp_path)
    result = orchestrator.run_once(base, "config/config.yaml")
    assert "timestamp" in result
    assert (base / "logs" / "decisions.log").exists()
    assert (base / "logs" / "pnl.log").exists()


def test_fail_closed_blocks_new_entries(tmp_path: Path, monkeypatch) -> None:
    base = _prepare_base(tmp_path)
    monkeypatch.setattr(orchestrator, "validate_snapshot_sync", lambda _snapshot: False)
    result = orchestrator.run_once(base, "config/config.yaml")
    assert result["allow_new_entries"] is False
    violations = (base / "logs" / "violations.log").read_text(encoding="utf-8").strip().splitlines()
    assert violations
    last = json.loads(violations[-1])
    assert "TIME_SYNC_UNMATCHED" in last["reasons"]


def test_idempotency_prevents_duplicate_entry(tmp_path: Path) -> None:
    base = _prepare_base(tmp_path)
    config = yaml.safe_load((base / "config" / "config.yaml").read_text(encoding="utf-8"))
    positions = load_positions(base)
    idempotency = load_idempotency(base)
    trades = load_trades(base)
    system_state = load_system_state(base)
    prices = {"AAPL": 180.0}
    signal = {
        "timestamp": "2026-04-06T00:00:00+00:00",
        "engine": "stock",
        "strategy": "stock_regime_momentum",
        "asset": "AAPL",
        "action": "enter",
        "side": "long",
        "signal_type": "momentum",
        "regime": "risk_on",
        "score": 0.9,
        "atr": 1.0,
        "price": 180.0,
        "stop_price": 177.0,
        "reason": "test",
        "orderbook": {"top3_ratio": 0.1, "spread_pct": 0.1},
    }

    positions, idempotency, trades, _ = execute_orders(
        base_dir=base,
        mode="paper",
        signals=[signal],
        positions=positions,
        idempotency=idempotency,
        trades=trades,
        system_state=system_state,
        prices=prices,
        config=config,
        allow_new_entries=True,
        block_reason="",
    )
    assert "AAPL" in positions

    positions, idempotency, trades, _ = execute_orders(
        base_dir=base,
        mode="paper",
        signals=[signal],
        positions=positions,
        idempotency=idempotency,
        trades=trades,
        system_state=system_state,
        prices=prices,
        config=config,
        allow_new_entries=True,
        block_reason="",
    )
    assert len(idempotency["seen"]) == 1


def test_watchlist_selector_generation(tmp_path: Path, monkeypatch) -> None:
    base = _prepare_base(tmp_path)
    config = yaml.safe_load((base / "config" / "config.yaml").read_text(encoding="utf-8"))
    system_state = load_system_state(base)

    monkeypatch.setattr(orchestrator, "should_run_stock_selector", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(orchestrator, "should_run_crypto_selector", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(orchestrator, "select_stock_watchlist", lambda _cfg: ["AAPL", "MSFT"])
    monkeypatch.setattr(orchestrator, "select_crypto_watchlist", lambda _cfg: ["BTCUSDT", "ETHUSDT"])

    orchestrator._run_selectors(base, config, datetime.now(timezone.utc), system_state)
    save_system_state(base, system_state)

    stock_active = load_json_state(base, "watchlist_stock_active.json", {"symbols": []})
    crypto_active = load_json_state(base, "watchlist_crypto_active.json", {"symbols": []})
    assert "AAPL" in stock_active["symbols"]
    assert "BTCUSDT" in crypto_active["symbols"]


def test_recovery_state_load_and_reconcile(tmp_path: Path) -> None:
    base = _prepare_base(tmp_path)
    stale_ts = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    system_state = load_system_state(base)
    system_state["last_heartbeat"] = stale_ts
    save_system_state(base, system_state)

    (base / "state" / "run.lock").write_text("locked", encoding="utf-8")
    save_pending_orders(base, [{"order_id": "p1", "status": "pending"}])

    result = recover(base, stale_minutes=5, mode="paper")
    assert result["stale"] is True
    assert result["lock_cleared"] is True
    pending = load_pending_orders(base)
    assert pending[0]["status"] == "reconciled"
