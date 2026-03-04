from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.backend.api import analytics, messages, processing, search, settings, sources, tags
from app.backend.db.schema import init_postgres_schema
from app.backend.db.session import engine
from app.config import get_settings

app = FastAPI(title="Personal AI News Intelligence API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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
app.include_router(settings.router)
app.mount("/miniapp", StaticFiles(directory="app/miniapp", html=True), name="miniapp")


@app.on_event("startup")
async def startup() -> None:
    await init_postgres_schema(engine)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": get_settings().app_name}
