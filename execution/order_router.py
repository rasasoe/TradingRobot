from __future__ import annotations

from typing import Any


def route_order(mode: str, signal: dict[str, Any]) -> dict[str, Any]:
    if mode == "paper":
        return {"status": "filled", "fill_price": float(signal["price"]), "order_id": f"paper-{signal['asset']}-{signal['timestamp']}"}
    # live_small skeleton: same behavior unless broker integration added
    if mode == "live_small":
        return {"status": "filled", "fill_price": float(signal["price"]), "order_id": f"live-small-{signal['asset']}-{signal['timestamp']}"}
    return {"status": "rejected", "reason": "unknown_mode"}
