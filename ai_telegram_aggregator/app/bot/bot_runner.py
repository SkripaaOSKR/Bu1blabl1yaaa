from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time

import httpx
from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler
from telegram.constants import ParseMode

from app.config import get_settings

# Настройка логгера
logger = logging.getLogger(__name__)
settings = get_settings()

# Умное определение хоста: 'api' для Docker (prod), '127.0.0.1' для локальных тестов
API_HOST = "api" if settings.environment == "prod" else "127.0.0.1"
API_BASE = f"http://{API_HOST}:{settings.api_port}"

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (БЕЗОПАСНОСТЬ И API) ---

def _admin(update: Update) -> bool:
    """Проверяет, является ли пользователь администратором (ID из .env)."""
    user = update.effective_user
    return bool(user and user.id in settings.admin_ids)


def _telegram_auth_header(user_id: int) -> str:
    """Генерирует зашифрованный заголовок для авторизации в нашем API."""
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
    """Универсальный метод для выполнения запросов к внутреннему API сервера."""
    admin_id = user_id or (next(iter(settings.admin_ids)) if settings.admin_ids else 0)
    headers = {"X-Telegram-Auth": _telegram_auth_header(admin_id)}
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.request(method, f"{API_BASE}{path}", json=payload, headers=headers)
        response.raise_for_status()
        return response.json()


async def _send_long_message(update: Update, text: str, **kwargs) -> None:
    """Отправка длинных сообщений с разбивкой по лимитам Telegram (4096 символов)."""
    max_len = 4000  # Чуть меньше лимита для надежности
    
    if len(text) <= max_len:
        await update.message.reply_text(text, **kwargs)
        return

    # Разбиваем текст, стараясь не резать слова (по переносу строки)
    parts = []
    while len(text) > 0:
        if len(text) > max_len:
            split_index = text.rfind('\n', 0, max_len)
            if split_index == -1:
                split_index = max_len
            parts.append(text[:split_index])
            text = text[split_index:].lstrip()
        else:
            parts.append(text)
            break

    for part in parts:
        await update.message.reply_text(part, **kwargs)
        await asyncio.sleep(0.3)  # Защита от спам-лимитов самого Telegram


# --- ИНИЦИАЛИЗАЦИЯ И МЕНЮ ---

async def post_init(application: Application) -> None:
    """Автоматически настраивает список команд в кнопке 'Меню' при запуске."""
    commands = [
        BotCommand("start", "🚀 Главная и статус"),
        BotCommand("help", "📖 Инструкция"),
        BotCommand("add_channel", "➕ Добавить канал (@name [ветка])"),
        BotCommand("list_channels", "📋 Список источников"),
        BotCommand("remove_channel", "❌ Удалить источник (по ID)"),
        BotCommand("add_keyword", "🚫 Добавить стоп-слово"),
        BotCommand("list_keywords", "🔍 Список фильтров"),
        BotCommand("run", "⚡ Запустить сбор новостей"),
        BotCommand("status", "⚙️ Статус воркера"),
        BotCommand("stats", "📊 Статистика за день"),
        BotCommand("search", "🔍 Поиск по базе"),
    ]
    await application.bot.set_my_commands(commands)
    logger.info("Bot commands menu has been updated.")


# --- ОБРАБОТЧИКИ ТЕКСТОВЫХ КОМАНД ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /start: приветствие и кнопка Mini App."""
    if not _admin(update):
        await update.message.reply_text(f"⛔ Доступ запрещен. Твой ID: {update.effective_user.id}")
        return

    # Добавляем красивую кнопку для открытия админки внутри Telegram
    keyboard = InlineKeyboardMarkup.from_button(
        InlineKeyboardButton(
            text="🎛 Открыть панель управления", 
            web_app=WebAppInfo(url=settings.miniapp_base_url)
        )
    )

    await update.message.reply_text(
        f"<b>🤖 AI AGGREGATOR 2.0 АКТИВЕН</b>\n\n"
        f"👤 <b>Ваш ID:</b> <code>{update.effective_user.id}</code>\n\n"
        f"Используйте меню или кнопку ниже для управления системой.",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /help: подробный гайд по всем функциям."""
    if not _admin(update): return
    
    help_text = (
        "📖 <b>Инструкция по управлению:</b>\n\n"
        "<b>1. Каналы-источники:</b>\n"
        "<code>/add_channel @username</code> — в общую ленту\n"
        "<code>/add_channel @username 15</code> — в ветку с ID 15\n"
        "<code>/list_channels</code> — список всех доноров\n"
        "<code>/remove_channel ID</code> — удалить источник\n\n"
        "<b>2. Гибкий фильтр (Имба):</b>\n"
        "<code>/add_keyword слово</code> — добавить в фильтр спама\n"
        "<code>/list_keywords</code> — список всех стоп-слов\n"
        "<code>/toggle_keyword ID</code> — вкл/выкл слово\n"
        "<code>/remove_keyword ID</code> — удалить слово навсегда\n\n"
        "<b>3. Работа с данными:</b>\n"
        "<code>/run</code> — собрать новости за 24 часа\n"
        "<code>/status</code> — состояние воркера\n"
        "<code>/stats</code> — итоги за сегодня\n"
        "<code>/search текст</code> — поиск по базе"
    )
    await update.message.reply_text(help_text, parse_mode=ParseMode.HTML)


async def add_channel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Добавление нового канала с поддержкой веток."""
    if not update.message: return 
    if not _admin(update): return
    
    if not context.args:
        await update.message.reply_text("⚠️ Формат: <code>/add_channel @username [ID_ветки]</code>", parse_mode=ParseMode.HTML)
        return
        
    channel = context.args[0]
    topic_id = int(context.args[1]) if len(context.args) > 1 and context.args[1].isdigit() else None
    
    try:
        data = await _api("POST", "/api/sources", {"channel": channel, "topic_id": topic_id}, update.effective_user.id)
        res_msg = f"✅ <b>Канал добавлен!</b>\n🔗 <code>{data['channel']}</code>"
        if data.get('topic_id'):
            res_msg += f"\n📁 Ветка: <code>{data['topic_id']}</code>"
        await update.message.reply_text(res_msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка API: {e}")


async def list_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Список всех источников (с поддержкой длинных списков)."""
    if not _admin(update): return
    try:
        rows = await _api("GET", "/api/sources", user_id=update.effective_user.id)
        if not rows:
            await update.message.reply_text("📭 Список источников пуст.")
            return
        msg = "📋 <b>Ваши источники:</b>\n\n"
        for r in rows:
            status = "🟢" if r['is_active'] else "🔴"
            topic = f" [Ветка: {r['topic_id']}]" if r.get('topic_id') else ""
            msg += f"{status} <b>ID {r['id']}</b>: {r['channel']}{topic}\n"
        
        await _send_long_message(update, msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def remove_channel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Удаление источника по ID."""
    if not _admin(update) or not context.args:
        await update.message.reply_text("⚠️ Укажите ID. Пример: <code>/remove_channel 5</code>", parse_mode=ParseMode.HTML)
        return
    try:
        await _api("DELETE", f"/api/sources/{int(context.args[0])}", user_id=update.effective_user.id)
        await update.message.reply_text("🗑 Источник удален.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


# --- ЛОГИКА ГИБКОГО ФИЛЬТРА (ИМБА) ---

async def add_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Добавление стоп-слова в базу."""
    if not _admin(update) or not context.args:
        await update.message.reply_text("⚠️ Введите слово: <code>/add_keyword казино</code>", parse_mode=ParseMode.HTML)
        return
    word = context.args[0].lower()
    try:
        data = await _api("POST", "/api/spam_keywords", {"word": word}, update.effective_user.id)
        await update.message.reply_text(f"🚫 Слово <b>'{data['word']}'</b> добавлено в фильтр.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def list_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Список всех стоп-слов (с поддержкой длинных списков)."""
    if not _admin(update): return
    try:
        rows = await _api("GET", "/api/spam_keywords", user_id=update.effective_user.id)
        if not rows:
            await update.message.reply_text("📭 Список фильтров пуст.")
            return
        msg = "🚫 <b>Активные фильтры:</b>\n\n"
        for r in rows:
            status = "✅" if r['is_active'] else "⚪"
            msg += f"{status} ID: <code>{r['id']}</code> | <b>{r['word']}</b>\n"
        msg += "\n<i>Используйте /toggle_keyword ID для переключения</i>"
        
        await _send_long_message(update, msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def toggle_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Включение/выключение стоп-слова."""
    if not _admin(update) or not context.args: return
    try:
        kw_id = int(context.args[0])
        await _api("PATCH", f"/api/spam_keywords/{kw_id}/toggle", user_id=update.effective_user.id)
        await update.message.reply_text(f"🔄 Статус фильтра {kw_id} изменен.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def remove_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Удаление стоп-слова навсегда."""
    if not _admin(update) or not context.args: return
    try:
        kw_id = int(context.args[0])
        await _api("DELETE", f"/api/spam_keywords/{kw_id}", user_id=update.effective_user.id)
        await update.message.reply_text(f"🗑 Слово удалено из фильтра.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


# --- СИСТЕМНЫЕ КОМАНДЫ ---

async def run_batch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запуск воркера."""
    if not _admin(update): return
    hours = int(context.args[0]) if context.args and context.args[0].isdigit() else 24
    await update.message.reply_text(f"⏳ Запускаю сканирование за {hours} ч...")
    try:
        result = await _api("POST", "/api/processing/run", {"hours": hours}, update.effective_user.id)
        await update.message.reply_text(f"🚀 Воркер запущен! Статус: <b>{result.get('status')}</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка запуска: {e}")


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Статус воркера."""
    if not _admin(update): return
    try:
        data = await _api("GET", "/api/processing/status", user_id=update.effective_user.id)
        state = data.get('state', {})
        is_running = "🟢 Активен" if data.get('running') else "💤 Спит"
        text = (
            f"⚙️ <b>Статус системы:</b>\n\n"
            f"Воркер: {is_running}\n"
            f"Последний запуск: <code>{state.get('last_run_duration', 0):.1f} сек.</code>\n"
            f"Обработано постов: <code>{state.get('last_run_count', 0)}</code>\n"
            f"Результат: <code>{state.get('last_run_status', 'N/A')}</code>"
        )
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Аналитика за сегодня."""
    if not _admin(update): return
    try:
        data = await _api("GET", "/api/analytics/daily", user_id=update.effective_user.id)
        daily = data.get('daily', [])
        if not daily:
            await update.message.reply_text("📊 Данных за сегодня еще нет.")
            return
        today = daily[0]
        text = (
            f"📊 <b>Итоги за сегодня:</b>\n\n"
            f"📥 Всего найдено: <code>{today.get('total', 0)}</code>\n"
            f"🗑 Удалено дублей: <code>{today.get('duplicates', 0)}</code>\n"
            f"✅ Опубликовано: <code>{today.get('published', 0)}</code>"
        )
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Семантический поиск."""
    if not _admin(update) or not context.args:
        await update.message.reply_text("⚠️ Введите запрос: <code>/search биткоин</code>", parse_mode=ParseMode.HTML)
        return
    query = " ".join(context.args)
    try:
        rows = await _api("POST", "/api/search", {"query": query, "limit": 3}, update.effective_user.id)
        if not rows:
            await update.message.reply_text("📭 Ничего не найдено.")
            return
        msg = f"🔍 <b>Результаты поиска по «{query}»:</b>\n\n"
        for r in rows:
            msg += f"🔹 <b>ID {r['id']}</b> (Сходство: {r['similarity']:.2f})\n{r['text'][:200]}...\n\n"
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка поиска: {e}")


# --- ОБРАБОТКА КНОПОК (КАРАНТИН) ---

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка кнопок под сообщениями в ветке СПАМ (текст и медиа)."""
    query = update.callback_query
    if not _admin(update):
        await query.answer("Нет доступа", show_alert=True)
        return

    try:
        action, msg_id = query.data.split(":")
        await query.answer("Выполняю...")

        # Проверяем, есть ли медиафайл у сообщения
        is_media = bool(query.message.photo or query.message.video or query.message.document)
        current_text = query.message.caption if is_media else query.message.text
        
        # Защита от пустых текстов
        if not current_text:
            current_text = ""

        if action == "pub":
            await _api("POST", f"/api/messages/{msg_id}/republish", user_id=update.effective_user.id)
            new_text = current_text + "\n\n✅ <b>ОПУБЛИКОВАНО</b>"
        elif action == "spam":
            await _api("DELETE", f"/api/messages/{msg_id}", user_id=update.effective_user.id)
            new_text = current_text + "\n\n🗑 <b>ПОДТВЕРЖДЕНО КАК СПАМ</b>"
        else:
            return

        # Обновляем текст в зависимости от типа сообщения
        if is_media:
            await query.edit_message_caption(caption=new_text, parse_mode=ParseMode.HTML, reply_markup=None)
        else:
            await query.edit_message_text(text=new_text, parse_mode=ParseMode.HTML, reply_markup=None)
            
    except Exception as e:
        logger.error(f"Callback error: {e}")
        await query.answer(f"Ошибка: {e}", show_alert=True)


def main() -> None:
    """Запуск бота."""
    app = Application.builder().token(settings.telegram_bot_token).post_init(post_init).build()
    
    # Регистрация всех обработчиков
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("add_channel", add_channel))
    app.add_handler(CommandHandler("list_channels", list_channels))
    app.add_handler(CommandHandler("remove_channel", remove_channel))
    app.add_handler(CommandHandler("add_keyword", add_keyword))
    app.add_handler(CommandHandler("list_keywords", list_keywords))
    app.add_handler(CommandHandler("toggle_keyword", toggle_keyword))
    app.add_handler(CommandHandler("remove_keyword", remove_keyword))
    app.add_handler(CommandHandler("run", run_batch))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("search", search))
    
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    logger.info("Telegram Admin Bot 2.0 is starting...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()