from __future__ import annotations

import logging
import sentry_sdk
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Импортируем все наши API роутеры
from app.backend.api import (
    analytics, 
    messages, 
    processing, 
    search, 
    settings as settings_api, 
    sources, 
    tags,
    spam_keywords  # <--- НАШ НОВЫЙ РОУТЕР
)
from app.backend.api.deps import get_session
from app.backend.db.schema import init_postgres_schema
from app.backend.db.session import engine
from app.backend.services.processing_service import ProcessingService
from app.config import get_settings
from app.utils.logger import setup_logger

settings = get_settings()

# --- ИНИЦИАЛИЗАЦИЯ ЧЕРНОГО ЯЩИКА ---
# 1. Локальные логи (запись в файл logs/blackbox.log)
setup_logger(settings.log_level)
logger = logging.getLogger(__name__)

# 2. Облачные логи (Sentry)
if settings.sentry_dsn:
    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        environment=settings.environment,
        traces_sample_rate=1.0,  # Записывать 100% ошибок
    )
    logger.info("Sentry (Cloud Black Box) is initialized and active.")
# -----------------------------------

# Инициализация FastAPI приложения
app = FastAPI(
    title="Personal AI News Intelligence API", 
    version="2.0.0",
    description="Backend server for AI-powered news aggregation and filtering"
)

# --- НАСТРОЙКА CORS ---
# Позволяет Mini App (фронтенду) делать запросы к этому API
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        settings.miniapp_origin, 
        "http://localhost:3000", 
        "http://127.0.0.1:3000",
        "http://localhost:8000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ПОДКЛЮЧЕНИЕ РОУТЕРОВ ---
app.include_router(sources.router)
app.include_router(tags.router)
app.include_router(messages.router)
app.include_router(search.router)
app.include_router(analytics.router)
app.include_router(processing.router)
app.include_router(settings_api.router)
app.include_router(spam_keywords.router) # <--- ВКЛЮЧАЕМ ГИБКИЙ ФИЛЬТР

# Подключаем папку с админкой (Mini App) как статические файлы
# Теперь по адресу http://localhost:8000/miniapp/ будет открываться твой сайт
app.mount("/miniapp", StaticFiles(directory="app/miniapp", html=True), name="miniapp")


@app.on_event("startup")
async def startup() -> None:
    """Действия при запуске сервера."""
    logger.info("Initializing database schema and migrations...")
    # Создаем таблицы и добавляем новые колонки (topic_id, fmt_text и т.д.)
    await init_postgres_schema(engine)
    logger.info("Database is ready.")


@app.get("/health")
async def health(db: AsyncSession = Depends(get_session)) -> dict[str, object]:
    """Базовая проверка работоспособности системы."""
    processing_service = ProcessingService(db)
    # Проверяем, загружен ли векторный индекс FAISS
    await processing_service.ensure_initialized()

    sources_count = await db.execute(text("SELECT COUNT(*) FROM sources"))
    
    return {
        "status": "ok",
        "version": "2.0.0",
        "faiss_vectors": processing_service.faiss.ntotal,
        "sources_total": int(sources_count.scalar() or 0),
    }


@app.get("/system/health")
async def system_health(db: AsyncSession = Depends(get_session)) -> dict[str, object]:
    """Детальная диагностика всех компонентов системы."""
    try:
        # Проверка связи с БД
        await db.execute(text("SELECT 1"))
        db_status = "ok"
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        db_status = "error"

    processing_service = ProcessingService(db)
    await processing_service.ensure_initialized()

    # Получаем состояние последнего запуска воркера
    state_row = await db.execute(
        text("SELECT last_run_status, last_processed_timestamp, last_run_count FROM processing_state WHERE id=1")
    )
    state = state_row.fetchone()

    # Считаем активные каналы
    active_row = await db.execute(text("SELECT COUNT(*) FROM sources WHERE is_active=TRUE"))
    active_count = int(active_row.scalar() or 0)

    return {
        "database": db_status,
        "faiss_loaded": processing_service.faiss is not None,
        "faiss_vectors_count": processing_service.faiss.ntotal,
        "active_sources": active_count,
        "worker_state": {
            "last_status": state.last_run_status if state else None,
            "last_count": state.last_run_count if state else 0,
            "last_checkpoint": state.last_processed_timestamp.isoformat() if state and state.last_processed_timestamp else None,
        }
    }