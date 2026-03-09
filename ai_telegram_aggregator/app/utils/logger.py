"""Centralized logger setup."""
from __future__ import annotations

import logging
import sys
import os
from logging.handlers import RotatingFileHandler


def setup_logger(level: str = "INFO") -> None:
    """Configure root logger for structured console and file output (Black Box)."""
    
    # Создаем директорию для логов (наш "черный ящик"), если её нет
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    # Расширенный формат логов: Дата | Уровень | Файл:Строка | Сообщение
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 1. Хэндлер для вывода в консоль (Docker logs)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level.upper())

    # 2. Хэндлер для "Черного ящика" (запись в файл blackbox.log)
    # Максимум 10 МБ на файл, храним 5 последних файлов (итого 50 МБ истории)
    file_handler = RotatingFileHandler(
        filename=os.path.join(log_dir, "blackbox.log"),
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    # В черный ящик пишем всё максимально подробно (DEBUG), 
    # даже если в консоль выводим только INFO
    file_handler.setLevel(logging.DEBUG)

    # Настраиваем корневой логгер
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG) # Опускаем базу до DEBUG, чтобы file_handler получал всё
    
    # Очищаем старые хэндлеры, чтобы не было дублей при перезапуске (например, в uvicorn)
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
        
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    # --- Фильтрация стороннего шума ---
    # Эти библиотеки очень разговорчивые, затыкаем их до уровня WARNING, 
    # чтобы они не засоряли наш черный ящик
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)