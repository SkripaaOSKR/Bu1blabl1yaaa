# AI Telegram Semantic Aggregator

Асинхронный агрегатор Telegram-каналов с семантической дедупликацией, объединением новостей, переводом на русский, тегированием и публикацией в закрытый канал.

## Возможности

- Сбор 100+ каналов через Telethon (async)
- Период анализа 1-24 часа (CLI + bot)
- Семантические embeddings (MiniLM) + cosine similarity
- Правило дублей в окне 7 дней
- Объединение похожих новостей с источниками
- Перевод MarianMT в русский (заменяемый слой)
- Динамические теги (#...)
- Спам-фильтрация
- Хранение в SQLite (готовность к PostgreSQL)
- FAISS persistent index
- Batch embeddings (configurable, default 64)
- LRU кеш переводов + lazy-load MarianMT
- Синхронизация FAISS <-> SQLite через таблицу `faiss_mapping` с авто-восстановлением индекса
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
-> Merge Engine -> Translator -> Tag Generator -> Media Handler -> Publisher -> Storage
```
