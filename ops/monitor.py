from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CUR = Path(__file__).resolve().parent
if sys.path and Path(sys.path[0]).resolve() == CUR:
    sys.path.pop(0)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from state.store import load_json_state


def monitor_status(base_dir: Path) -> dict:
    system = load_json_state(base_dir, "system_state.json", {})
    w_stock = load_json_state(base_dir, "watchlist_stock_active.json", {"symbols": []})
    w_crypto = load_json_state(base_dir, "watchlist_crypto_active.json", {"symbols": []})
    alert_state = load_json_state(base_dir, "alert_idempotency.json", {"seen": []})

    run_lock = (base_dir / "state" / "run.lock").exists()
    fast_lock = (base_dir / "state" / "fast.lock").exists()

    return {
        "last_heartbeat": system.get("last_heartbeat"),
        "fast_heartbeat": system.get("fast_heartbeat"),
        "safe_mode": system.get("safe_mode", False),
        "violation_streak": system.get("violation_streak", 0),
        "drift_last_warning": system.get("drift_last_warning", "unknown"),
        "run_lock": run_lock,
        "fast_lock": fast_lock,
        "watchlist_stock_count": len(w_stock.get("symbols", [])),
        "watchlist_crypto_count": len(w_crypto.get("symbols", [])),
        "alert_seen_count": len(alert_state.get("seen", [])),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor trading system status")
    parser.add_argument("--base-dir", default=".")
    args = parser.parse_args()
    print(json.dumps(monitor_status(Path(args.base_dir).resolve()), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
