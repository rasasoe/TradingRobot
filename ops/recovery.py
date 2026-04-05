from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CUR = Path(__file__).resolve().parent
if sys.path and Path(sys.path[0]).resolve() == CUR:
    sys.path.pop(0)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.reconciliation import reconcile_orders
from state.store import load_system_state


def recover(base_dir: Path, stale_minutes: int = 5, mode: str = "paper") -> dict:
    system = load_system_state(base_dir)
    hb = system.get("last_heartbeat")
    stale = True
    if hb:
        last = datetime.fromisoformat(str(hb)).astimezone(timezone.utc)
        stale = (datetime.now(timezone.utc) - last) > timedelta(minutes=stale_minutes)

    recon = reconcile_orders(base_dir, mode)
    run_lock = base_dir / "state" / "run.lock"
    if stale and run_lock.exists():
        run_lock.unlink(missing_ok=True)

    return {"stale": stale, "reconciliation": recon, "lock_cleared": stale}


def main() -> None:
    parser = argparse.ArgumentParser(description="Recovery and reconciliation")
    parser.add_argument("--base-dir", default=".")
    parser.add_argument("--mode", default="paper")
    args = parser.parse_args()
    print(json.dumps(recover(Path(args.base_dir).resolve(), mode=args.mode), ensure_ascii=False))


if __name__ == "__main__":
    main()
