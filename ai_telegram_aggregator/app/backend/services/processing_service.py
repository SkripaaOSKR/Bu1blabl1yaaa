from __future__ import annotations

import asyncio
import logging
import time
import re
import httpx
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import numpy as np
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from telethon.errors import FloodWaitError

from app.collector.telegram_client import CollectedMessage, TelegramCollector
from app.config import get_settings
from app.backend.services.faiss_store import FaissStore
from app.backend.services.embedding_service import EmbeddingService
from app.backend.services.data_service import DataService
from app.backend.services.ai_service import AIService
from app.backend.services.nlp import DuplicateEngine, MergeEngine, Preprocessor, SpamFilter, TagGenerator

logger = logging.getLogger(__name__)


class ProcessingService:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db
        self.settings = get_settings()
        self.preprocessor = Preprocessor()
        self.spam_filter = SpamFilter(
            active_keywords=set(),
            min_words=self.settings.spam_min_words,
            max_links=self.settings.spam_max_links,
            repeat_threshold=self.settings.spam_repeat_threshold,
        )
        self.embedding_service = EmbeddingService.get_instance(
            self.settings.embedding_model_name, 
            self.settings.embedding_batch_size
        )
        
        self.faiss = FaissStore.get_instance(self.settings.faiss_index_path)
        self.dedupe = DuplicateEngine(self.settings.dedupe_similarity_threshold, self.faiss)
        self.merger = MergeEngine(self.settings.merge_max_chars)
        self.tagger = TagGenerator()
        self.ai_service = AIService()
        
        self.cancelled = False
        self._initialized = False
        self.compiled_spam_patterns = []

    async def ensure_initialized(self) -> None:
        if self._initialized:
            return
        await self.sync_faiss_index()
        self._initialized = True

    async def sync_faiss_index(self) -> None:
        db_count_row = await self.db.execute(text("SELECT COUNT(*) FROM embeddings"))
        db_count = int(db_count_row.scalar() or 0)
        faiss_count = self.faiss.ntotal
        if db_count == faiss_count:
            return
        logger.warning("FAISS desync detected: db=%s faiss=%s. Rebuilding index.", db_count, faiss_count)
        await self.faiss.rebuild_from_db(self.db)

    async def _load_source_map(self) -> dict[str, dict]:
        rows = await self.db.execute(
            text("SELECT id, channel, topic_id FROM sources WHERE is_active=TRUE ORDER BY priority, id")
        )
        return {str(r.channel): {"id": int(r.id), "topic_id": r.topic_id} for r in rows.fetchall()}

    async def _get_source_checkpoints(self) -> dict[int, datetime]:
        rows = await self.db.execute(
            text("SELECT source_id, MAX(created_at) as last_msg FROM messages GROUP BY source_id")
        )
        checkpoints = {}
        for r in rows.fetchall():
            if r.last_msg:
                dt = r.last_msg
                checkpoints[r.source_id] = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
        return checkpoints

    async def _encode_chunked(self, texts: list[str]) -> np.ndarray:
        if not texts:
            return np.empty((0, getattr(self.embedding_service, 'dimension', 384)), dtype=np.float32)
            
        tasks = []
        chunk_size = max(1, self.settings.embedding_batch_size)
        
        for i in range(0, len(texts), chunk_size):
            chunk = texts[i : i + chunk_size]
            tasks.append(asyncio.to_thread(self.embedding_service.encode, chunk))
            
        vectors = await asyncio.gather(*tasks)
        return np.vstack(vectors)

    # Старый метод сохранен на всякий случай, чтобы ничего не сломать
    async def _save_processed(self, source_id: int, msg: CollectedMessage, language: str, vector: np.ndarray, is_dup: bool, score: float, tags: list[str], is_spam: bool = False, text_hash: str = "") -> tuple[int, bool]:
        try:
            emb_row = await self.db.execute(
                text("INSERT INTO embeddings(vector) VALUES (:v) RETURNING id"), 
                {"v": vector.tobytes()}
            )
            emb_id = int(emb_row.fetchone().id)
            
            message_row = await self.db.execute(
                text(
                    """
                    INSERT INTO messages(source_id, text, fmt_text, created_at, processed_at, embedding_id, language, is_duplicate, is_published, similarity_score, media_group_id, media_type, original_message_id, is_confirmed_spam, text_hash)
                    VALUES (:sid, :text, :fmt, :created, :processed, :eid, :lang, :dup, FALSE, :score, :mg, :mt, :orig_id, :is_spam, :thash)
                    RETURNING id
                    """
                ),
                {
                    "sid": source_id, "text": msg.text, "fmt": msg.fmt_text,
                    "created": msg.created_at, "processed": datetime.now(timezone.utc),
                    "eid": emb_id, "lang": language, "dup": is_dup, "score": score,
                    "mg": msg.media_group_id, "mt": msg.media_type, "orig_id": msg.message_id,
                    "is_spam": is_spam, "thash": text_hash
                },
            )
            message_id = int(message_row.fetchone().id)
            await self.db.flush()
        except Exception:
            await self.db.rollback()
            raise

        self.faiss.add_with_ids(vector.reshape(1, -1), np.asarray([emb_id], dtype=np.int64))
        
        for tag in tags:
            await self.db.execute(text("INSERT INTO tags(name) VALUES (:n) ON CONFLICT(name) DO NOTHING"), {"n": tag})
            await self.db.execute(text("UPDATE tags SET usage_count=usage_count+1 WHERE name=:n"), {"n": tag})
            await self.db.execute(
                text("INSERT INTO message_tags(message_id, tag_id) SELECT :mid, id FROM tags WHERE name=:n ON CONFLICT DO NOTHING"), 
                {"mid": message_id, "n": tag}
            )
        
        await self.db.flush()
        return message_id, is_dup

    async def _publish_beautiful_post(self, collector: TelegramCollector, channel_name: str, messages: list[dict], topic_id: int | None = None, is_spam: bool = False, is_update: bool = False, update_text: str = None, reply_to_msg_id: int = None, ai_data: dict = None, bypass_ai: bool = False) -> int | bool:
        if not messages:
            return False

        settings = get_settings()
        target = settings.telegram_publish_channel
        
        forced_topic_id = None
        if isinstance(target, str):
            target = target.strip()
            if "/" in target:
                parts = target.split("/")
                if len(parts) == 2 and parts[0].lstrip('-').isdigit() and parts[1].isdigit():
                    target = int(parts[0])
                    forced_topic_id = int(parts[1])
            elif target.lstrip('-').isdigit():
                target = int(target)

        raw_text = "\n\n".join([m['text'] for m in messages if m['text']])
        if not raw_text: raw_text = "Новость без текста"
        
        final_caption = ""
        target_topic = topic_id 
        if forced_topic_id:
            target_topic = forced_topic_id

        clean_channel = str(channel_name).replace('@', '')
        orig_msg_id = messages[0].get('original_message_id', '')
        
        if clean_channel.startswith('-100'):
            source_url = f"https://t.me/c/{clean_channel[4:]}/{orig_msg_id}"
        elif clean_channel.startswith('+') or 'joinchat' in clean_channel:
            source_url = f"https://t.me/{clean_channel}"
        else:
            source_url = f"https://t.me/{clean_channel}/{orig_msg_id}"

        source_link = f'<a href="{source_url}">🔗 Источник</a>'

        if is_spam:
            if settings.telegram_spam_topic_id:
                target_topic = settings.telegram_spam_topic_id
                final_caption = f"⚠️ <b>КАРАНТИН (СПАМ-ФИЛЬТР)</b>\n\n{raw_text[:900]}\n\n{source_link}"
            else:
                return False 
                
        elif is_update and update_text:
            final_caption = f"{update_text}\n\n{source_link}"
            
        else:
            if ai_data and ai_data.get("rewrite"):
                caption_base = ai_data["rewrite"]
                tags = ai_data.get("tags", [])
                tag_string = ""
                if tags:
                    clean_tags = [t if t.startswith('#') else f"#{t}" for t in tags]
                    tag_string = "\n\n" + " ".join(clean_tags)
                
                final_caption = f"{caption_base}\n\n{source_link}{tag_string}"
            else:
                text_msg = next((m for m in messages if m['fmt_text']), None)
                caption_base = text_msg['fmt_text'] if text_msg else raw_text
                
                tag_rows = await self.db.execute(
                    text("""
                        SELECT t.name FROM tags t 
                        JOIN message_tags mt ON t.id = mt.tag_id 
                        WHERE mt.message_id = :mid AND t.is_blocked = FALSE LIMIT 5
                    """),
                    {"mid": messages[0]['id']}
                )
                tags = [row.name for row in tag_rows.fetchall()]
                tag_string = ""
                if tags:
                    tag_string = "\n\n#" + "".join(filter(str.isalnum, channel_name.replace('@', ''))) + " " + " ".join(tags)
                
                final_caption = f"{caption_base}\n\n{source_link}{tag_string}"

        has_media = any(m.get('media_type') for m in messages)
        is_long_caption = len(final_caption) > 1024 and has_media
        media_caption = "" if is_long_caption else final_caption

        reply_to = reply_to_msg_id if is_update else target_topic
        
        target_channel = messages[0]['source_channel']
        if isinstance(target_channel, str):
            if target_channel.lstrip('-').isdigit():
                target_channel = int(target_channel)
            elif target_channel.startswith('+'):
                target_channel = f"https://t.me/{target_channel}"

        sent_media_msg = None 

        for attempt in range(1, 4):
            try:
                if has_media and not sent_media_msg:
                    if len(messages) > 1:
                        media_files = []
                        msg_ids = [m['original_message_id'] for m in messages]
                        orig_messages = await collector.client.get_messages(target_channel, ids=msg_ids)
                        
                        for orig in orig_messages:
                            if orig and orig.media:
                                media_files.append(orig.media)
                                
                        if media_files:
                            sent_media_msg = await collector.client.send_file(
                                target, media_files, caption=media_caption, parse_mode='html', reply_to=reply_to
                            )
                    
                    elif messages[0]['media_type']:
                        orig = await collector.client.get_messages(target_channel, ids=messages[0]['original_message_id'])
                        if orig and orig.media:
                            sent_media_msg = await collector.client.send_file(
                                target, orig.media, caption=media_caption, parse_mode='html', reply_to=reply_to
                            )
                
                if is_long_caption or not has_media:
                    text_reply_to = sent_media_msg[0].id if isinstance(sent_media_msg, list) else (sent_media_msg.id if sent_media_msg else reply_to)
                    
                    if final_caption.strip():
                        sent_text_msg = await collector.client.send_message(
                            target, final_caption, parse_mode='html', link_preview=False, reply_to=text_reply_to
                        )
                        if not sent_media_msg:
                            sent_media_msg = sent_text_msg

                break 

            except FloodWaitError as exc:
                if exc.seconds > 300: 
                    logger.error(f"FloodWait {exc.seconds}s. Skipping.")
                    return False
                await asyncio.sleep(exc.seconds)
            except Exception:
                logger.exception("Publish error on attempt %s", attempt)
                await asyncio.sleep(2 ** attempt)
        
        if sent_media_msg:
            pub_id = sent_media_msg[0].id if isinstance(sent_media_msg, list) else sent_media_msg.id
            
            # Если пост попал в карантин, бот пришлет к нему твои кнопки управления
            if is_spam:
                db_msg_id = messages[0]['id']
                bot_token = self.settings.telegram_bot_token
                if bot_token:
                    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                    keyboard = {"inline_keyboard": [
                        [{"text": "🔗 Источник (Оригинал)", "url": source_url}],
                        [{"text": "✅ Опубликовать", "callback_data": f"pub:{db_msg_id}"},
                         {"text": "🚫 В спам (Обучить)", "callback_data": f"spam:{db_msg_id}"}]
                    ]}
                    payload = {
                        "chat_id": target,
                        "reply_to_message_id": pub_id,
                        "text": "⚙️ <b>Выберите действие:</b>",
                        "parse_mode": "HTML",
                        "reply_markup": keyboard
                    }
                    if target_topic: payload["message_thread_id"] = target_topic
                    
                    async def send_btns():
                        async with httpx.AsyncClient() as client:
                            await client.post(url, json=payload)
                    asyncio.create_task(send_btns())
                    
            return pub_id
            
        return False

    async def process_specific_messages(self, message_ids: list[int]) -> bool:
        """
        [NEW] ФАЗА 2: ВОРКЕР (ПОТРЕБИТЕЛЬ)
        Обрабатывает конкретные сообщения: ИИ, дедупликация и публикация.
        """
        if not message_ids: 
            return False
            
        # УЛУЧШЕНИЕ 1: Гарантируем, что FAISS инициализирован в отдельном процессе
        await self.ensure_initialized()
            
        # 1. Загружаем сырые сообщения из базы
        rows = await self.db.execute(
            text("SELECT * FROM messages WHERE id = ANY(:ids) ORDER BY id"),
            {"ids": message_ids}
        )
        db_msgs = rows.fetchall()
        if not db_msgs: 
            return False
            
        source_id = db_msgs[0].source_id
        
        src_row = await self.db.execute(
            text("SELECT channel, topic_id FROM sources WHERE id = :sid"),
            {"sid": source_id}
        )
        src = src_row.fetchone()
        if not src: 
            return False
        channel_name = src.channel
        topic_id = src.topic_id

        full_text = "\n\n".join([m.text for m in db_msgs if m.text]).strip()
        full_fmt_text = "\n\n".join([m.fmt_text or m.text for m in db_msgs]).strip()
        
        # УЛУЧШЕНИЕ 6: Защита от пустых текстов (например, одни фото в альбоме)
        if not full_text:
            logger.info(f"Messages {message_ids} contain no text. Skipping processing.")
            return True

        # 2. Настраиваем фильтры
        data_svc = DataService(self.db)
        active_stop_words = await data_svc.get_active_spam_keywords()
        self.compiled_spam_patterns = [re.compile(rf"\b{re.escape(w)}\b", re.IGNORECASE) for w in active_stop_words]
        self.spam_filter = SpamFilter(
            active_keywords=active_stop_words,
            min_words=self.settings.spam_min_words,
            max_links=self.settings.spam_max_links,
            repeat_threshold=self.settings.spam_repeat_threshold,
        )
        
        processed = self.preprocessor.run(full_text)
        processed_fmt = self.preprocessor.run(full_fmt_text)
        
        # 3. ПРОВЕРКА НА СПАМ
        is_spam = False
        if processed_fmt.cleaned:
            text_lower = processed_fmt.cleaned.lower()
            for pattern in self.compiled_spam_patterns:
                if pattern.search(text_lower):
                    is_spam = True
                    break
        if not is_spam and processed_fmt.cleaned:
            has_media = any(m.media_type for m in db_msgs)
            if not has_media and self.spam_filter.is_spam(processed_fmt.cleaned):
                is_spam = True
                
        msgs_dict = []
        for m in db_msgs:
            msgs_dict.append({
                'id': m.id, 'text': m.text, 'fmt_text': m.fmt_text, 
                'media_type': m.media_type, 'original_message_id': m.original_message_id, 
                'source_channel': channel_name
            })

        if is_spam:
            await self.db.execute(text("UPDATE messages SET is_confirmed_spam=TRUE, processed_at=NOW() WHERE id = ANY(:ids)"), {"ids": message_ids})
            await self.db.execute(text("UPDATE sources SET spam_count=spam_count+1 WHERE id=:sid"), {"sid": source_id})
            await self.db.commit()
            if self.settings.telegram_spam_topic_id:
                async with TelegramCollector(self.settings.telegram_api_id, self.settings.telegram_api_hash, self.settings.telegram_session_name, self.settings.telegram_string_session) as collector:
                    await self._publish_beautiful_post(collector, channel_name, msgs_dict, topic_id, is_spam=True)
            return True

        # 4. ДЕДУПЛИКАЦИЯ УРОВЕНЬ 1 (BLAKE2B HASH)
        text_hash = self.dedupe.get_text_hash(processed.cleaned) if processed.cleaned else ""
        is_exact_dup = False
        if text_hash:
            res = await self.db.execute(text("SELECT id FROM messages WHERE text_hash = :th AND id != ANY(:ids) LIMIT 1"), {"th": text_hash, "ids": message_ids})
            is_exact_dup = res.fetchone() is not None

        if is_exact_dup:
            await self.db.execute(text("UPDATE messages SET is_duplicate=TRUE, text_hash=:th, processed_at=NOW() WHERE id = ANY(:ids)"), {"th": text_hash, "ids": message_ids})
            await self.db.execute(text("UPDATE sources SET duplicate_count=duplicate_count+1 WHERE id=:sid"), {"sid": source_id})
            await self.db.commit()
            return True

        # 5. ДЕДУПЛИКАЦИЯ УРОВЕНЬ 2 (FAISS)
        # УЛУЧШЕНИЕ 2: Динамический размер embedding'а
        dim = getattr(self.embedding_service, 'dimension', 384)
        vector = np.zeros(dim, dtype=np.float32)
        is_dup, score = False, 0.0
        
        if processed.cleaned:
            vectors = await self._encode_chunked([processed.cleaned])
            vector = vectors[0]
            is_dup, _, score = await self.dedupe.check_status(vector, self.db)

        emb_row = await self.db.execute(text("INSERT INTO embeddings(vector) VALUES (:v) RETURNING id"), {"v": vector.tobytes()})
        emb_id = emb_row.scalar()
        
        tags = self.tagger.generate(processed.cleaned) if processed.cleaned else []

        await self.db.execute(
            text("""
                UPDATE messages 
                SET embedding_id=:eid, text_hash=:th, similarity_score=:score, is_duplicate=:dup, language=:lang, processed_at=NOW() 
                WHERE id = ANY(:ids)
            """),
            {"eid": emb_id, "th": text_hash, "score": score, "dup": is_dup, "lang": processed.language, "ids": message_ids}
        )
        
        for tag in tags:
            await self.db.execute(text("INSERT INTO tags(name) VALUES (:n) ON CONFLICT(name) DO NOTHING"), {"n": tag})
            await self.db.execute(text("UPDATE tags SET usage_count=usage_count+1 WHERE name=:n"), {"n": tag})
            await self.db.execute(text("INSERT INTO message_tags(message_id, tag_id) SELECT :mid, id FROM tags WHERE name=:n ON CONFLICT DO NOTHING"), {"mid": message_ids[0], "n": tag})

        self.faiss.add_with_ids(vector.reshape(1, -1), np.asarray([emb_id], dtype=np.int64))
        await self.db.commit()

        # 6. ИИ (REWRITE / UPDATES)
        update_text = None
        is_update = False
        reply_to_msg_id = None
        ai_data = None

        settings_db = await data_svc.get_settings()
        custom_prompt = settings_db.get('ai_prompt')

        if is_dup:
            await self.db.execute(text("UPDATE sources SET duplicate_count=duplicate_count+1 WHERE id=:sid"), {"sid": source_id})
            await self.db.commit()
            
            query_vec = np.ascontiguousarray(vector.reshape(1, -1), dtype=np.float32)
            _, ids = self.faiss.search(query_vec, k=1)
            
            # УЛУЧШЕНИЕ 3: Безопасное получение best_eid из FAISS
            best_eid = int(ids[0][0]) if ids.size else -1
            if best_eid <= 0:
                best_eid = None
                
            if best_eid:
                old_msg_res = await self.db.execute(text("SELECT text, published_message_id, created_at FROM messages WHERE embedding_id = :eid AND is_published = TRUE LIMIT 1"), {"eid": best_eid})
                old_msg = old_msg_res.fetchone()
                if old_msg and old_msg.published_message_id and (datetime.now(timezone.utc) - old_msg.created_at < timedelta(days=7)):
                    async with httpx.AsyncClient(timeout=45.0) as http_client:
                        self.ai_service = AIService(client=http_client, custom_prompt=custom_prompt)
                        update_text = await self.ai_service.generate_update(old_msg.text, full_text)
                        if update_text:
                            is_update = True
                            reply_to_msg_id = old_msg.published_message_id
                        else:
                            return True
        else:
            async with httpx.AsyncClient(timeout=45.0) as http_client:
                self.ai_service = AIService(client=http_client, custom_prompt=custom_prompt)
                has_media = any(m.media_type for m in db_msgs)
                ai_data = await self.ai_service.edit_news(full_fmt_text, has_media)
                
                if ai_data:
                    if ai_data.get("is_spam"):
                        await self.db.execute(text("UPDATE messages SET is_confirmed_spam=TRUE WHERE id=ANY(:ids)"), {"ids": message_ids})
                        await self.db.commit()
                        if self.settings.telegram_spam_topic_id:
                            async with TelegramCollector(self.settings.telegram_api_id, self.settings.telegram_api_hash, self.settings.telegram_session_name, self.settings.telegram_string_session) as collector:
                                await self._publish_beautiful_post(collector, channel_name, msgs_dict, topic_id, is_spam=True)
                        return True
                    elif ai_data.get("importance", 1.0) < 0.3:
                        return True

        # 7. ФИНАЛЬНАЯ ПУБЛИКАЦИЯ
        pub_id = False
        async with TelegramCollector(self.settings.telegram_api_id, self.settings.telegram_api_hash, self.settings.telegram_session_name, self.settings.telegram_string_session) as collector:
            pub_id = await self._publish_beautiful_post(
                collector, channel_name, msgs_dict, topic_id=topic_id,
                is_spam=False, is_update=is_update,
                update_text=update_text, reply_to_msg_id=reply_to_msg_id,
                ai_data=ai_data
            )
        
        if pub_id:
            await self.db.execute(text("UPDATE messages SET is_published=TRUE, published_message_id=:pub_id WHERE id = ANY(:ids)"), {"ids": message_ids, "pub_id": pub_id})
            await self.db.execute(text("UPDATE sources SET published_messages=published_messages+1 WHERE id=:sid"), {"sid": source_id})
            await self.db.commit()

        try:
            self.faiss.persist()
        except Exception as e:
            logger.warning(f"Failed to persist FAISS index: {e}")
            
        return True

    async def _enqueue_batch(self, batch: list[tuple[int, CollectedMessage]], reverse_source_map: dict[int, str]) -> tuple[datetime | None, int]:
        """
        [NEW] ФАЗА 2: СБОРЩИК (ПРОИЗВОДИТЕЛЬ)
        Сохраняет сырые сообщения в БД и кидает задачу в очередь Redis.
        """
        newest = None
        albums = defaultdict(list)
        for source_id, msg in batch:
            if newest is None or msg.created_at > newest:
                newest = msg.created_at
            key = msg.media_group_id if msg.media_group_id else f"single_{msg.message_id}"
            albums[key].append((source_id, msg))

        processed_count = 0
        from app.tasks import process_news_task

        for key, items in albums.items():
            source_id = items[0][0]
            msgs = [m for s, m in items]
            saved_ids = []
            
            for msg in msgs:
                row = await self.db.execute(
                    text("""
                    INSERT INTO messages(source_id, text, fmt_text, created_at, media_group_id, media_type, original_message_id)
                    VALUES (:sid, :text, :fmt, :created, :mg, :mt, :orig_id)
                    RETURNING id
                    """),
                    {
                        "sid": source_id,
                        "text": msg.text,
                        "fmt": msg.fmt_text,
                        "created": msg.created_at,
                        "mg": msg.media_group_id,
                        "mt": msg.media_type,
                        "orig_id": msg.message_id
                    }
                )
                saved_ids.append(row.scalar())
            
            await self.db.commit()
            
            # УЛУЧШЕНИЕ 4: Fallback на случай падения Redis
            if saved_ids:
                try:
                    await process_news_task.kiq(saved_ids)
                    processed_count += len(saved_ids)
                except Exception:
                    logger.exception(f"Failed to enqueue job for messages {saved_ids}")

        return newest, processed_count

    async def run_batch(self, hours: int | None = None) -> dict[str, int | str]:
        """Облегченный цикл Сборщика. Только качает и кидает в очередь."""
        await self.ensure_initialized()
        
        # Просто обновляем статус для дашборда, БЕЗ блокировок
        await self.db.execute(text("""
            UPDATE processing_state 
            SET last_run_status = 'running', updated_at = NOW()
            WHERE id = 1
        """))
        await self.db.commit()

        started = time.perf_counter()
        status, processed_total, newest = "success", 0, None
        
        try:
            source_map = await self._load_source_map()
            reverse_source_map = {v['id']: k for k, v in source_map.items()}
            checkpoints = await self._get_source_checkpoints()
            default_since = datetime.now(timezone.utc) - timedelta(hours=hours or 24)

            try:
                async with TelegramCollector(
                    self.settings.telegram_api_id, 
                    self.settings.telegram_api_hash, 
                    self.settings.telegram_session_name,
                    self.settings.telegram_string_session
                ) as collector:
                    
                    for channel_name, source_data in source_map.items():
                        if self.cancelled: status = "cancelled"; break
                        await asyncio.sleep(0.5)
                        sid = source_data['id']
                        
                        start_time = datetime.now(timezone.utc) - timedelta(hours=hours) if hours is not None else checkpoints.get(sid, default_since)
                            
                        current_batch = []
                        channel_processed = 0
                        
                        async for msg in collector.iter_messages([channel_name], hours or 24, since_timestamp=start_time):
                            if self.cancelled: break
                            current_batch.append((sid, msg))
                            
                            if len(current_batch) >= self.settings.batch_size:
                                n, cnt = await self._enqueue_batch(current_batch, reverse_source_map)
                                processed_total += cnt
                                channel_processed += cnt
                                if n and (newest is None or n > newest): newest = n
                                current_batch = []

                        if current_batch and not self.cancelled:
                            n, cnt = await self._enqueue_batch(current_batch, reverse_source_map)
                            processed_total += cnt
                            channel_processed += cnt
                            if n and (newest is None or n > newest): newest = n

                        # Обновляем статистику скачиваний для источника
                        if channel_processed > 0:
                            await self.db.execute(
                                text("UPDATE sources SET total_messages = total_messages + :cnt, last_scan_at = NOW() WHERE id = :id"),
                                {"cnt": channel_processed, "id": sid}
                            )
                            await self.db.commit()

            except Exception:
                status = "failed"
                logger.exception("Processing failed")
                try:
                    await self.db.rollback()
                except Exception as rollback_err:
                    logger.error(f"Rollback failed: {rollback_err}")
            
        except Exception as e:
            status = "failed"
            logger.exception("Critical error outside main loop")
        finally:
            duration = time.perf_counter() - started
            checkpoint = newest.astimezone(timezone.utc) if (newest and status == "success") else None
            
            try:
                await self.db.execute(
                    text("""
                    UPDATE processing_state 
                    SET last_run_status=:s, last_run_duration=:d, last_run_count=:c, 
                        last_processed_timestamp=COALESCE(:ts, last_processed_timestamp), updated_at=NOW() 
                    WHERE id=1
                    """),
                    {"s": status, "d": duration, "c": processed_total, "ts": checkpoint}
                )
                await self.db.commit()
            except Exception as e:
                logger.error(f"CRITICAL: Failed to release processing lock: {e}")
        
        return {"status": status, "processed": processed_total, "duration_sec": round(duration, 3)}
    
    async def _send_admin_buttons(self, chat_id: str | int, reply_to_id: int, db_msg_id: int, source_url: str) -> None:
        """Отправляет кнопки управления в Карантин через официального бота."""
        bot_token = self.settings.telegram_bot_token
        if not bot_token: 
            return
            
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        keyboard = {
            "inline_keyboard": [
                [{"text": "🔗 Посмотреть оригинал", "url": source_url}],
                [
                    {"text": "✅ Опубликовать", "callback_data": f"pub:{db_msg_id}"},
                    {"text": "🚫 В спам (Обучить)", "callback_data": f"spam:{db_msg_id}"}
                ]
            ]
        }
        payload = {
            "chat_id": chat_id,
            "reply_to_message_id": reply_to_id,
            "text": "⚙️ <b>Действие с постом:</b>",
            "parse_mode": "HTML",
            "reply_markup": keyboard
        }
        
        async with httpx.AsyncClient() as client:
            try:
                await client.post(url, json=payload)
            except Exception as e:
                logger.error(f"Failed to send bot buttons: {e}")

    def cancel(self) -> None:
        self.cancelled = True