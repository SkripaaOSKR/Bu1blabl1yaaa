from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time

import httpx
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, ContextTypes

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()
API_BASE = f"http://api:{settings.api_port}"


def _admin(update: Update) -> bool:
    user = update.effective_user
    return bool(user and user.id in settings.admin_ids)


def _telegram_auth_header(user_id: int) -> str:
    user_obj = {"id": user_id, "username": "bot_admin"}
    payload = {
        "auth_date": int(time.time()),
        "query_id": f"bot-{user_id}",
        "user": json.dumps(user_obj, separators=(",", ":")),
    }
    data_check = "\n".join([f"{k}={v}" for k, v in sorted(payload.items())])
    secret = hashlib.sha256(settings.telegram_bot_token.encode()).digest()
    payload["hash"] = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
    return json.dumps(payload)


async def _api(method: str, path: str, payload: dict | None = None, user_id: int | None = None) -> dict | list:
    admin_id = user_id or (next(iter(settings.admin_ids)) if settings.admin_ids else 0)
    headers = {"X-Telegram-Auth": _telegram_auth_header(admin_id)}
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.request(method, f"{API_BASE}{path}", json=payload, headers=headers)
        response.raise_for_status()
        return response.json()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _admin(update):
        return
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Open Control Panel", web_app={"url": settings.miniapp_base_url})]]
    )
    await update.message.reply_text("Admin bot ready.", reply_markup=keyboard)


async def add_channel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _admin(update):
        return
    if not context.args:
        await update.message.reply_text("Usage: /add_channel @channel")
        return
    data = await _api("POST", "/api/sources", {"channel": context.args[0]}, update.effective_user.id)
    await update.message.reply_text(f"Added: {data['channel']}")


async def remove_channel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _admin(update) or not context.args:
        return
    await _api("DELETE", f"/api/sources/{int(context.args[0])}", user_id=update.effective_user.id)
    await update.message.reply_text("Removed")


async def list_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _admin(update):
        return
    rows = await _api("GET", "/api/sources", user_id=update.effective_user.id)
    text = "\n".join([f"{r['id']}: {r['channel']} active={r['is_active']}" for r in rows]) or "No channels"
    await update.message.reply_text(text)


async def allow_tag(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _admin(update) or not context.args:
        return
    await _api(
        "POST",
        "/api/tags/toggle",
        {"name": context.args[0], "is_allowed": True, "is_blocked": False},
        update.effective_user.id,
    )
    await update.message.reply_text("Allowed")


async def block_tag(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _admin(update) or not context.args:
        return
    await _api(
        "POST",
        "/api/tags/toggle",
        {"name": context.args[0], "is_allowed": False, "is_blocked": True},
        update.effective_user.id,
    )
    await update.message.reply_text("Blocked")


async def search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _admin(update) or not context.args:
        return
    rows = await _api("POST", "/api/search", {"query": " ".join(context.args), "limit": 5}, update.effective_user.id)
    text = "\n\n".join([f"#{r['id']} score={r['similarity']:.3f}\n{r['text'][:300]}" for r in rows]) or "No results"
    await update.message.reply_text(text)


async def run_batch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _admin(update):
        return
    hours = int(context.args[0]) if context.args else None
    result = await _api("POST", "/api/processing/run", {"hours": hours}, update.effective_user.id)
    await update.message.reply_text(str(result))


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _admin(update):
        return
    data = await _api("GET", "/api/processing/status", user_id=update.effective_user.id)
    await update.message.reply_text(str(data))


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _admin(update):
        return
    data = await _api("GET", "/api/analytics/daily", user_id=update.effective_user.id)
    await update.message.reply_text(f"daily_points={len(data['daily'])} top_sources={len(data['top_sources'])}")


def main() -> None:
    app = Application.builder().token(settings.telegram_bot_token).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("add_channel", add_channel))
    app.add_handler(CommandHandler("remove_channel", remove_channel))
    app.add_handler(CommandHandler("list_channels", list_channels))
    app.add_handler(CommandHandler("allow_tag", allow_tag))
    app.add_handler(CommandHandler("block_tag", block_tag))
    app.add_handler(CommandHandler("search", search))
    app.add_handler(CommandHandler("run", run_batch))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("stats", stats))
    logger.info("Starting Telegram admin bot")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
