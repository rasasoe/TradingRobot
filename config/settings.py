from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


def load_config(base_dir: Path, config_path: str = "config/config.yaml") -> dict[str, Any]:
    path = base_dir / config_path
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    telegram = cfg.setdefault("notifications", {}).setdefault("telegram", {})
    env_token = os.getenv("TRADING_TELEGRAM_BOT_TOKEN", "").strip()
    env_chat = os.getenv("TRADING_TELEGRAM_CHAT_ID", "").strip()
    if env_token:
        telegram["bot_token"] = env_token
    if env_chat:
        telegram["chat_id"] = env_chat
    return cfg
