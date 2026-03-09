import logging
import asyncio
import json
from typing import Optional
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from app.config import get_settings

logger = logging.getLogger(__name__)

class AIService:
    def __init__(self, client: Optional[httpx.AsyncClient] = None, custom_prompt: str = None):
        self.settings = get_settings()
        self.api_key = self.settings.groq_api_key
        # Рекомендуется Llama-3.3-70b-versatile, так как она отлично работает с JSON
        self.model = "llama-3.3-70b-versatile" 
        self.url = "https://api.groq.com/openai/v1/chat/completions"
        
        self._client = client
        self.custom_prompt = custom_prompt
        
        # ЗАЩИТА ОТ ЛИМИТОВ GROQ (Максимум 3 одновременных запроса)
        self.semaphore = asyncio.Semaphore(3)

    def _build_system_prompt(self, has_media: bool) -> str:
        """Собирает системный промпт с требованием строгого JSON-ответа."""
        
        base_rules = (
            "Ты — профессиональный AI-редактор и аналитик новостей.\n"
            "Твоя задача: проанализировать сырой текст, сделать качественный рерайт, "
            "присвоить теги, категорию и оценить важность новости.\n\n"
            "ПРАВИЛА РЕРАЙТА:\n"
            "- Сохрани 100% фактов, имен и дат.\n"
            "- В исходном тексте могут быть HTML-ссылки (вида <a href=\"...\">текст</a>). Ты ОБЯЗАН перенести эти теги <a> в свой рерайт.\n"
            "- Стиль: инфостиль, без воды.\n\n"
        )
        
        if has_media:
            base_rules += "Текст будет подписью к картинке. Сделай его коротким (до 800 символов), 2-3 абзаца.\n\n"
        else:
            base_rules += "Текст будет обычным постом. Сделай его читаемым (1000-1500 символов), разбей на абзацы.\n\n"

        # Требование JSON формата
        json_rules = (
            "ВЫХОДНОЙ ФОРМАТ:\n"
            "Ты ОБЯЗАН вернуть ответ в формате строгого JSON. Никакого лишнего текста до или после JSON.\n"
            "Структура JSON:\n"
            "{\n"
            '  "rewrite": "Готовый переписанный текст новости",\n'
            '  "tags": ["тег1", "тег2", "тег3"],\n'
            '  "category": "одна из: politics, economy, tech, crypto, military, sports, incident, other",\n'
            '  "importance": 0.0 до 1.0 (где 1.0 - это срочная мировая новость, а 0.1 - локальный мусор),\n'
            '  "is_spam": true или false (true если это реклама казино, ставок, скам-крипты, продажа курсов),\n'
            '  "spam_reason": "краткая причина, если is_spam=true, иначе null"\n'
            "}\n"
        )
        
        return base_rules + json_rules

    def _build_update_prompt(self) -> str:
        """Промпт для генерации дополнения (Reply) к старой новости."""
        return (
            "Ты — профессиональный новостной редактор.\n"
            "Сравни СТАРУЮ новость и НОВУЮ новость, найди новые факты и напиши короткое дополнение.\n\n"
            "ПРАВИЛА:\n"
            "1. Только новые факты (до 500 символов).\n"
            "2. Начни со слов: ⚡️ UPD:\n"
            "3. Если в новой новости НЕТ новых фактов, ответь ровно одним словом: SKIP\n"
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
        reraise=True
    )
    async def _make_request(self, client: httpx.AsyncClient, payload: dict) -> httpx.Response:
        response = await client.post(
            self.url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=45.0
        )
        response.raise_for_status() 
        return response

    async def edit_news(self, text: str, has_media: bool = False) -> dict | None:
        """Главный метод для рерайта и анализа текста. Возвращает словарь (JSON)."""
        if not self.api_key or not text or len(text.strip()) < 5:
            return None 

        system_prompt = self._build_system_prompt(has_media)
        
        payload = {
            "model": self.model,
            "messages":[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Сырой текст:\n\n{text}"}
            ],
            "temperature": 0.2, # Низкая температура для стабильного JSON
            "max_tokens": 1500,
            "response_format": {"type": "json_object"} # ФОРСИРУЕМ JSON
        }

        async with self.semaphore:
            try:
                if self._client:
                    response = await self._make_request(self._client, payload)
                else:
                    async with httpx.AsyncClient() as temp_client:
                        response = await self._make_request(temp_client, payload)
                
                data = response.json()
                choices = data.get('choices',[])
                if not choices:
                    return None
                    
                result_text = choices[0].get('message', {}).get('content', '')
                
                if not result_text:
                    return None
                    
                # Очистка от маркдауна (если ИИ обернул ответ в ```json ... ```)
                result_text = result_text.strip('` \n').replace('json\n', '', 1)
                    
                # Парсим JSON-строку в Python словарь
                result_json = json.loads(result_text)
                
                # 1. Проверка структуры JSON
                required_keys = ["rewrite", "tags", "category", "importance"]
                for key in required_keys:
                    if key not in result_json:
                        logger.error(f"AI Service: Отсутствует обязательное поле '{key}' в ответе ИИ.")
                        return None
                
                # 2. Ограничить длину rewrite (защита от галлюцинаций с бесконечным текстом)
                rewrite = result_json.get("rewrite", "")
                if rewrite and len(rewrite) > 2000:
                    result_json["rewrite"] = rewrite[:2000]
                    
                # 3. Нормализовать importance (защита от выхода за границы и неверных типов данных)
                try:
                    importance = float(result_json.get("importance", 0.0))
                    result_json["importance"] = max(0.0, min(1.0, importance))
                except (ValueError, TypeError):
                    logger.warning("AI Service: ИИ вернул нечисловое значение для importance, установлено 0.5")
                    result_json["importance"] = 0.5
                    
                return result_json

            except json.JSONDecodeError as e:
                logger.error(f"AI Service: ИИ вернул невалидный JSON: {e}\nТекст ответа: {result_text}")
                return None
            except Exception as e:
                logger.error(f"Ошибка в AI Service (edit_news): {e}")
                return None

    async def generate_update(self, old_text: str, new_text: str) -> str | None:
        """Метод для создания дополнения (Reply). Остается как было (возвращает строку)."""
        if not self.api_key or not old_text or not new_text:
            return None

        system_prompt = self._build_update_prompt()
        user_content = f"СТАРАЯ НОВОСТЬ:\n{old_text}\n\nНОВАЯ НОВОСТЬ:\n{new_text}"
        
        payload = {
            "model": self.model,
            "messages":[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content}
            ],
            "temperature": 0.3,
            "max_tokens": 800
        }

        async with self.semaphore:
            try:
                if self._client:
                    response = await self._make_request(self._client, payload)
                else:
                    async with httpx.AsyncClient() as temp_client:
                        response = await self._make_request(temp_client, payload)
                
                data = response.json()
                choices = data.get('choices',[])
                if not choices:
                    return None
                    
                result = choices[0].get('message', {}).get('content', '')
                result = result.strip() if result else None
                
                if result == "SKIP":
                    return None
                    
                return result

            except Exception as e:
                logger.error(f"Ошибка при генерации апдейта: {e}")
                return None