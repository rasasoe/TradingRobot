from __future__ import annotations

from pathlib import Path


def rotate_logs(base_dir: Path, max_mb: int, keep: int) -> None:
    logs_dir = base_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    limit = max_mb * 1024 * 1024

    for path in logs_dir.glob("*.log"):
        if not path.exists() or path.stat().st_size < limit:
            continue
        for i in range(keep, 0, -1):
            src = logs_dir / f"{path.name}.{i}"
            dst = logs_dir / f"{path.name}.{i+1}"
            if src.exists():
                if i == keep:
                    src.unlink(missing_ok=True)
                else:
                    src.rename(dst)
        path.rename(logs_dir / f"{path.name}.1")
        path.touch()
