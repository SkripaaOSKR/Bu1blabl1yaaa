# AI Telegram Semantic Aggregator

Асинхронный агрегатор Telegram-каналов с семантической дедупликацией, объединением новостей, тегированием и публикацией в закрытый канал.

## Возможности

- Сбор 100+ каналов через Telethon (async)
- Период анализа 1-24 часа (CLI + bot)
- Семантические embeddings (MiniLM) + cosine similarity
- Дедупликация в окне 7 дней
- Batch embeddings (`EMBEDDING_BATCH_SIZE=64`)
- Явный mapping `embedding_id ↔ faiss_id` и автосинхронизация FAISS
- Объединение похожих новостей с удалением дублей абзацев и источниками
- Спам-фильтрация (v1 эвристики)
- Хранение в SQLite (готовность к PostgreSQL)
- FAISS persistent index
- Аналитика в `data/analytics.json`

## Быстрый старт

```bash
cd ai_telegram_aggregator
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python main.py --hours 12
```

## .env

```env
TELEGRAM_API_ID=12345
TELEGRAM_API_HASH=your_hash
TELEGRAM_SESSION_NAME=aggregator
TELEGRAM_SOURCES=@channel1,@channel2
TELEGRAM_PUBLISH_CHANNEL=@private_target_channel
LOG_LEVEL=INFO
BATCH_SIZE=200
EMBEDDING_BATCH_SIZE=64
```

## CLI

- `python main.py --hours 12`
- или интерактивное меню (1/2/5/7/12/20/24 часа)

## Bot команды

- `/run 12`
- `/status`
- `/stats`

## Архитектура

```text
Collector -> Batch Processor -> Preprocessor -> Semantic Engine -> Duplicate Detector
-> Merge Engine -> Tag Generator -> Media Handler -> Publisher -> Storage
```
