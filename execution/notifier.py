from __future__ import annotations

import json
from typing import Any
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen


def send_telegram_message(bot_token: str, chat_id: str, text: str, timeout: float = 5.0) -> bool:
    if not bot_token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = Request(url, data=payload, method="POST", headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            return '"ok":true' in body
    except (URLError, HTTPError, TimeoutError):
        return False


def format_signal_alert(signal: dict[str, Any], status: str) -> str:
    action = signal.get("action", "unknown")
    side = signal.get("side", "unknown")
    action_kr = "매수" if action == "enter" and side == "long" else "매도" if action == "exit" else action
    return (
        "[신호 알림]\n"
        f"자산: {signal.get('asset', 'UNKNOWN')}\n"
        f"전략엔진: {signal.get('engine', 'unknown')}\n"
        f"행동: {action_kr} ({action} {side})\n"
        f"신호유형: {signal.get('signal_type', 'signal')}\n"
        f"점수: {signal.get('score', 0.0)}\n"
        f"레짐: {signal.get('regime', 'unknown')}\n"
        f"상태: {status}"
    )


def format_system_alert(reasons: list[str]) -> str:
    return (
        "[시스템 경고]\n"
        "ENFORCEMENT 규칙 위반\n"
        f"사유: {'|'.join(reasons)}\n"
        "신규 진입 차단"
    )


def format_risk_alert(drift_warning: str) -> str:
    return (
        "[리스크 경고]\n"
        f"상태: {drift_warning}\n"
        "노출 축소 권고"
    )
