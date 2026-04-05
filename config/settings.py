from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        return {}
    return data


def load_config(base_dir: Path, config_path: str = "config/config.yaml") -> dict[str, Any]:
    cfg = _load_yaml(base_dir / config_path)

    secrets = _load_yaml(base_dir / "config" / "secrets.yaml")
    telegram = cfg.setdefault("notifications", {}).setdefault("telegram", {})
    secret_tg = secrets.get("telegram", {}) if isinstance(secrets.get("telegram", {}), dict) else {}

    token = os.getenv("TRADING_TELEGRAM_BOT_TOKEN", str(secret_tg.get("bot_token", ""))).strip()
    chat_id = os.getenv("TRADING_TELEGRAM_CHAT_ID", str(secret_tg.get("chat_id", ""))).strip()

    if token:
        telegram["bot_token"] = token
    if chat_id:
        telegram["chat_id"] = chat_id

    return cfg
