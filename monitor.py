from __future__ import annotations

import argparse
import json
from pathlib import Path


def _last_jsonl(path: Path) -> dict | None:
    if not path.exists():
        return None
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    if not lines:
        return None
    return json.loads(lines[-1])


def monitor_once(base_dir: Path) -> dict:
    dec = _last_jsonl(base_dir / "logs" / "decisions.log")
    vio = _last_jsonl(base_dir / "logs" / "violations.log")

    alerts = []
    if dec and dec.get("executed") is True:
        alerts.append(f"signal_executed:{dec['asset']}")
    if vio:
        alerts.append(f"violation:{'|'.join(vio.get('reasons', []))}")

    result = {"alerts": alerts, "latest_decision": dec, "latest_violation": vio}
    print(result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple monitor")
    parser.add_argument("--base-dir", default=".")
    args = parser.parse_args()
    monitor_once(Path(args.base_dir).resolve())


if __name__ == "__main__":
    main()
