from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import yaml


def _get_updates(token: str, timeout: float = 8.0) -> dict:
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    req = Request(url, method="GET")
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _validate_token(token: str) -> str:
    t = token.strip()
    if not t:
        raise SystemExit("bot token is empty")
    if "<" in t or ">" in t or " " in t:
        raise SystemExit(
            "invalid bot token. Do not use placeholder text. "
            "Use real token like 123456789:AA..."
        )
    if ":" not in t:
        raise SystemExit("invalid bot token format")
    return t


def _extract_latest_chat_id(payload: dict) -> str:
    results = payload.get("result", [])
    latest_update_id = -1
    latest_chat_id = ""
    for item in results:
        update_id = int(item.get("update_id", -1))
        msg = item.get("message") or item.get("edited_message") or {}
        chat = msg.get("chat", {})
        chat_id = chat.get("id")
        if chat_id is None:
            continue
        if update_id > latest_update_id:
            latest_update_id = update_id
            latest_chat_id = str(chat_id)
    return latest_chat_id


def _save_config(config_path: Path, bot_username: str, bot_token: str, chat_id: str, enabled: bool) -> None:
    with config_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    tg = cfg.setdefault("notifications", {}).setdefault("telegram", {})
    tg["bot_username"] = bot_username
    tg["bot_token"] = bot_token
    tg["chat_id"] = chat_id
    tg["enabled"] = enabled

    with config_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, allow_unicode=False, sort_keys=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Setup Telegram bot config")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--bot-username", default="tradingrasbot")
    parser.add_argument("--bot-token", default="")
    parser.add_argument("--chat-id", default="")
    parser.add_argument("--enable", action="store_true")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    token = args.bot_token.strip() or os.getenv("TRADING_TELEGRAM_BOT_TOKEN", "").strip()
    token = _validate_token(token)
    chat_id = args.chat_id.strip()

    if not chat_id:
        try:
            payload = _get_updates(token)
            chat_id = _extract_latest_chat_id(payload)
        except (URLError, HTTPError, TimeoutError) as exc:
            raise SystemExit(f"Failed to call Telegram API: {exc}") from exc

    if not chat_id:
        raise SystemExit(
            "chat_id not found. Open @tradingrasbot chat and send /start once, then run setup again."
        )

    _save_config(config_path, args.bot_username, token, chat_id, enabled=args.enable)
    print({"ok": True, "bot_username": args.bot_username, "chat_id": chat_id, "config": str(config_path)})


if __name__ == "__main__":
    main()
