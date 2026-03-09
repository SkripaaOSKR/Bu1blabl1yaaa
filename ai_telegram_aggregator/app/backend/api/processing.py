from __future__ import annotations

import asyncio
import logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.api.deps import get_admin_user_id, get_session
from app.backend.db.session import SessionLocal
from app.backend.models.schemas import ProcessingRunRequest
from app.backend.services.data_service import DataService
from app.backend.services.processing_service import ProcessingService

router = APIRouter(prefix="/api/processing", tags=["processing"])

logger = logging.getLogger(__name__)

# Глобальные переменные для отслеживания активного процесса парсинга.
# Это позволяет нам знать статус воркера из любой точки программы.
_running_task: asyncio.Task | None = None
_running_service: ProcessingService | None = None


async def _run_with_own_session(hours: int | None) -> dict:
    """
    Внутренняя функция для запуска воркера. 
    Создает отдельную сессию БД, так как процесс парсинга может длиться долго,
    и основная сессия API может закрыться по таймауту.
    """
    global _running_service
    
    # Открываем новую сессию специально для этого запуска
    async with SessionLocal() as db:
        service = ProcessingService(db)
        _running_service = service
        
        try:
            logger.info(f"Starting background processing task (hours={hours})")
            # Запускаем основной алгоритм сбора и обработки
            result = await service.run_batch(hours)
            logger.info(f"Background processing task finished: {result}")
            return result
        except Exception as e:
            # Если что-то упало внутри, логируем ошибку, чтобы не уронить весь сервер
            logger.exception("CRITICAL: Background processing task failed")
            return {"status": "failed", "error": str(e)}
        finally:
            # В любом случае очищаем ссылку на сервис, чтобы можно было запустить снова
            _running_service = None


@router.post("/run")
async def run_batch(
    payload: ProcessingRunRequest, 
    db: AsyncSession = Depends(get_session), 
    user_id: int = Depends(get_admin_user_id)
) -> dict:
    """
    Запуск процесса сбора новостей.
    
    Проверки:
    1. Если воркер уже работает - выдает ошибку 409 (Conflict).
    2. Если свободен - создает фоновую задачу (asyncio.create_task).
    """
    global _running_task
    
    # Проверка на "двойной запуск"
    if _running_task and not _running_task.done():
        logger.warning(f"User {user_id} tried to start worker while it is already running")
        raise HTTPException(
            status_code=409, 
            detail="Процесс парсинга уже запущен и работает в фоне."
        )

    # Запускаем задачу в фоне, чтобы API сразу ответило "ОК", не дожидаясь конца парсинга
    _running_task = asyncio.create_task(_run_with_own_session(payload.hours))
    
    # Записываем в аудит, кто запустил воркер
    await DataService(db).log_action(user_id, "processing.run", payload.model_dump())
    
    return {"ok": True, "status": "started"}


@router.post("/cancel")
async def cancel_run(
    db: AsyncSession = Depends(get_session), 
    user_id: int = Depends(get_admin_user_id)
) -> dict:
    """
    Остановка текущего процесса парсинга.
    Посылает сигнал 'cancelled' внутрь ProcessingService.
    """
    global _running_service
    
    if _running_service is not None:
        # Вызываем метод отмены внутри сервиса
        _running_service.cancel()
        logger.info(f"User {user_id} sent cancel signal to worker")
        
        await DataService(db).log_action(user_id, "processing.cancel", {})
        return {"ok": True, "message": "Сигнал на остановку отправлен воркеру."}
    
    return {"ok": False, "message": "Сейчас нет активных процессов для остановки."}


@router.get("/status")
async def status(
    db: AsyncSession = Depends(get_session), 
    user_id: int = Depends(get_admin_user_id)
) -> dict:
    """
    Проверка статуса системы. 
    Возвращает информацию о том, работает ли бот сейчас, и данные о последнем запуске.
    """
    # Берем из базы данных последнюю статистику (длительность, кол-во постов)
    state = await DataService(db).get_processing_state()
    
    # Проверяем статус фоновой задачи
    is_running = _running_task is not None and not _running_task.done()
    
    return {
        "running": is_running,
        "state": state
    }