from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_config(base_dir: Path, config_path: str = "config/config.yaml") -> dict[str, Any]:
    path = base_dir / config_path
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)
