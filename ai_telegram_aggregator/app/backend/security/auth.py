from __future__ import annotations

import hashlib
import hmac
import json
import logging
from typing import Any

from fastapi import Header, HTTPException, status
from app.config import get_settings

# Загружаем настройки
settings = get_settings()
# Настраиваем логгер
logger = logging.getLogger(__name__)

def verify_telegram_login(payload: dict[str, Any]) -> bool:
    """
    Полная проверка цифровой подписи Telegram.
    Поддерживает два алгоритма шифрования:
    1. WebApp (HMAC-SHA256 с ключом WebAppData) - для браузера/телефона.
    2. Bot (HMAC-SHA256 с ключом SHA256(Token)) - для скриптов.
    """
    try:
        # Если в данных нет хеша - проверять нечего
        if "hash" not in payload:
            logger.warning("Auth Check: 'hash' field missing in payload.")
            return False
        
        check_hash = payload["hash"]
        
        # Сортируем ключи по алфавиту (требование Telegram)
        # Исключаем поле hash из проверки
        data_check_arr = []
        for k, v in sorted(payload.items()):
            if k != "hash":
                # Важно: приводим значения к строке, так как auth_date может быть int
                data_check_arr.append(f"{k}={v}")
        
        data_check_string = "\n".join(data_check_arr)
        data_check_bytes = data_check_string.encode()

        # --- АЛГОРИТМ 1: WebApp (Стандартный) ---
        secret_key_webapp = hmac.new(
            b"WebAppData", 
            settings.telegram_bot_token.encode(), 
            hashlib.sha256
        ).digest()
        
        hash_webapp = hmac.new(
            secret_key_webapp, 
            data_check_bytes, 
            hashlib.sha256
        ).hexdigest()

        if hmac.compare_digest(hash_webapp, check_hash):
            return True

        # --- АЛГОРИТМ 2: Bot (Альтернативный) ---
        secret_key_bot = hashlib.sha256(
            settings.telegram_bot_token.encode()
        ).digest()

        hash_bot = hmac.new(
            secret_key_bot, 
            data_check_bytes, 
            hashlib.sha256
        ).hexdigest()

        if hmac.compare_digest(hash_bot, check_hash):
            return True

        logger.warning(f"Auth Check: Signature mismatch. Hash: {check_hash}")
        return False

    except Exception as e:
        logger.error(f"Auth Check: Internal error during verification: {e}")
        return False


async def telegram_auth_guard(x_telegram_auth: str | None = Header(None)) -> int:
    """
    Главный шлюз безопасности API.
    
    Логика работы:
    1. Проверяем, запущен ли сервер локально (Dev Mode).
    2. Пытаемся честно проверить подпись Telegram.
    3. Если подпись верна -> пускаем.
    4. Если подпись НЕ верна (или её нет), НО мы на Localhost -> пускаем Админа (Bypass).
    5. Иначе -> Ошибка 403.
    """
    
    # 1. ОПРЕДЕЛЕНИЕ РЕЖИМА РАЗРАБОТКИ (LOCALHOST)
    base_url = settings.miniapp_base_url or ""
    is_dev_mode = any(h in base_url for h in ["localhost", "127.0.0.1", "0.0.0.0"])
    
    # Получаем ID главного админа для фоллбэка
    fallback_admin_id = 0
    if settings.admin_ids:
        fallback_admin_id = next(iter(settings.admin_ids))

    # Переменная для хранения найденного ID
    user_id: int | None = None
    auth_error_msg = "Unknown error"

    # 2. ПОПЫТКА ЧЕСТНОЙ АВТОРИЗАЦИИ
    if x_telegram_auth and x_telegram_auth != "null" and x_telegram_auth != "":
        try:
            # Парсим JSON
            auth_data = json.loads(x_telegram_auth)
            
            # Проверяем подпись
            if verify_telegram_login(auth_data):
                # Извлекаем данные пользователя
                user_field = auth_data.get("user")
                
                user_data = {}
                if isinstance(user_field, str):
                    user_data = json.loads(user_field)
                elif isinstance(user_field, dict):
                    user_data = user_field
                else:
                    # Иногда ID лежит в корне (для ботов)
                    user_data = auth_data
                
                if user_data.get("id"):
                    user_id = int(user_data.get("id"))
            else:
                auth_error_msg = "Invalid signature"
                
        except json.JSONDecodeError:
            auth_error_msg = "Invalid JSON format"
        except Exception as e:
            auth_error_msg = f"Exception: {str(e)}"
    else:
        auth_error_msg = "Header missing or empty"

    # 3. ЛОГИКА СПАСЕНИЯ (DEV MODE BYPASS)
    # Если честная авторизация не прошла (user_id всё еще None)
    if user_id is None:
        if is_dev_mode:
            if fallback_admin_id == 0:
                logger.error("CRITICAL: Localhost detected, but ADMIN_USER_IDS is empty in .env!")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Server config error: No admins"
                )
            
            # Мы дома, пускаем админа без проверки
            # logger.info(f"Auth Bypass: Localhost detected. Reason: {auth_error_msg}. Logging in as {fallback_admin_id}")
            return int(fallback_admin_id)
        else:
            # Мы на боевом сервере - никаких поблажек
            logger.warning(f"Auth Failed: {auth_error_msg}")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Authentication failed: {auth_error_msg}"
            )

    # 4. ФИНАЛЬНАЯ ПРОВЕРКА ПРАВ (WHITELIST)
    # Даже если подпись верна, пользователь должен быть в списке админов
    str_user_id = str(user_id)
    str_admin_ids = {str(uid) for uid in settings.admin_ids}

    if str_user_id not in str_admin_ids:
        # Если мы на локалхосте, но зашли под левым ID -> всё равно пускаем главного админа (для удобства)
        if is_dev_mode:
             return int(fallback_admin_id)
             
        logger.warning(f"Access Denied: User {str_user_id} is not in admin list.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You are not an administrator"
        )

    return int(user_id)