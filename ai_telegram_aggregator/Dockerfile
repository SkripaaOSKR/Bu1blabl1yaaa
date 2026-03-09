# Используем легкий образ Python
FROM python:3.11-slim

# Устанавливаем системные зависимости (нужны для компиляции FAISS и asyncpg)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем зависимости и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код проекта
COPY . .

# Указываем Python, где искать модули
ENV PYTHONPATH=/app

# По умолчанию ничего не запускаем (команды запуска прописаны в docker-compose)
CMD ["python", "--version"]