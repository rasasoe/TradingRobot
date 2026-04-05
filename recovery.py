from __future__ import annotations

import argparse
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

from state.store import load_pending_orders, load_system_state, save_pending_orders


def recover(base_dir: Path, stale_minutes: int = 3) -> dict:
    system = load_system_state(base_dir)
    last = system.get("last_heartbeat")
    stale = True

    if last:
        hb = datetime.fromisoformat(last).astimezone(timezone.utc)
        stale = datetime.now(timezone.utc) - hb > timedelta(minutes=stale_minutes)

    recovered_orders = 0
    if stale:
        subprocess.run(["python3", str(base_dir / "orchestrator.py"), "--base-dir", str(base_dir)], check=False)

    pending = load_pending_orders(base_dir)
    for p in pending:
        if p.get("status") == "pending":
            p["status"] = "recovered"
            recovered_orders += 1
    save_pending_orders(base_dir, pending)

    return {"stale": stale, "recovered_orders": recovered_orders}


def main() -> None:
    parser = argparse.ArgumentParser(description="Recovery process")
    parser.add_argument("--base-dir", default=".")
    args = parser.parse_args()
    print(recover(Path(args.base_dir).resolve()))


if __name__ == "__main__":
    main()
