from __future__ import annotations

from notifications.console_notifier import ConsoleNotifier
from notifications.telegram_notifier import TelegramNotifier


class NotificationRouter:
    def __init__(self, config: dict) -> None:
        telegram_cfg = config.get("notifications", {}).get("telegram", {})
        console_cfg = config.get("notifications", {}).get("console", {})
        self.telegram_enabled = bool(telegram_cfg.get("enabled", False))
        self.console_enabled = bool(console_cfg.get("enabled", True))
        self.telegram = TelegramNotifier(str(telegram_cfg.get("bot_token", "")), str(telegram_cfg.get("chat_id", "")))
        self.console = ConsoleNotifier()

    def send(self, text: str, force_console_fallback: bool = True) -> bool:
        sent = False
        if self.telegram_enabled:
            sent = self.telegram.send(text)
        if (not sent and force_console_fallback) and self.console_enabled:
            sent = self.console.send(text)
        return sent
