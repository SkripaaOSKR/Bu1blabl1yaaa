from __future__ import annotations

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.api import analytics, messages, processing, search, settings as settings_api, sources, tags
from app.backend.api.deps import get_session
from app.backend.db.schema import init_postgres_schema
from app.backend.db.session import engine
from app.backend.services.processing_service import ProcessingService
from app.config import get_settings

settings = get_settings()
app = FastAPI(title="Personal AI News Intelligence API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.miniapp_origin, "http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(sources.router)
app.include_router(tags.router)
app.include_router(messages.router)
app.include_router(search.router)
app.include_router(analytics.router)
app.include_router(processing.router)
app.include_router(settings_api.router)
app.mount("/miniapp", StaticFiles(directory="app/miniapp", html=True), name="miniapp")


@app.on_event("startup")
async def startup() -> None:
    await init_postgres_schema(engine)


@app.get("/health")
async def health(db: AsyncSession = Depends(get_session)) -> dict[str, object]:
    processing_service = ProcessingService(db)
    await processing_service.ensure_initialized()

    sources_row = await db.execute(text("SELECT COUNT(*) FROM sources"))
    return {
        "status": "ok",
        "faiss_vectors": processing_service.faiss.ntotal,
        "sources": int(sources_row.scalar() or 0),
    }


@app.get("/system/health")
async def system_health(db: AsyncSession = Depends(get_session)) -> dict[str, object]:
    try:
        await db.execute(text("SELECT 1"))
        db_status = "ok"
    except Exception:
        db_status = "error"

    processing_service = ProcessingService(db)
    await processing_service.ensure_initialized()

    state_row = await db.execute(
        text("SELECT last_run_status, last_processed_timestamp FROM processing_state WHERE id=1")
    )
    state = state_row.fetchone()

    active_row = await db.execute(text("SELECT COUNT(*) FROM sources WHERE is_active=TRUE"))
    active_count = int(active_row.scalar() or 0)

    return {
        "db": db_status,
        "faiss_loaded": processing_service.faiss is not None,
        "faiss_vectors": processing_service.faiss.ntotal,
        "sources_active": active_count,
        "last_run_status": state.last_run_status if state else None,
        "last_checkpoint": state.last_processed_timestamp.isoformat() if state and state.last_processed_timestamp else None,
    }
