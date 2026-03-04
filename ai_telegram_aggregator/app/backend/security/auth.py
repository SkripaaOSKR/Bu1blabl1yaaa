from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any

from fastapi import Header, HTTPException

from app.config import get_settings

settings = get_settings()


def verify_telegram_login(payload: dict[str, Any]) -> bool:
    data_check_arr = [f"{k}={v}" for k, v in sorted(payload.items()) if k != "hash"]
    data_check_string = "\n".join(data_check_arr)
    secret_key = hashlib.sha256(settings.telegram_bot_token.encode()).digest()
    expected = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, str(payload.get("hash", "")))


async def telegram_auth_guard(x_telegram_auth: str = Header(...)) -> int:
    try:
        payload = json.loads(x_telegram_auth)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=401, detail="Invalid telegram auth payload") from exc

    if not verify_telegram_login(payload):
        raise HTTPException(status_code=401, detail="Invalid telegram auth hash")

    user_payload = payload.get("user")
    if isinstance(user_payload, str):
        try:
            user_payload = json.loads(user_payload)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=401, detail="Invalid telegram auth user") from exc

    if not isinstance(user_payload, dict) or "id" not in user_payload:
        raise HTTPException(status_code=401, detail="Missing telegram user")

    user_id = int(user_payload["id"])
    if user_id not in settings.admin_ids:
        raise HTTPException(status_code=403, detail="Forbidden")

    return user_id
