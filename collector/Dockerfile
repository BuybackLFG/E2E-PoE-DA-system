FROM python:3.12.2-slim

WORKDIR /app

# Устанавливаем системные зависимости для работы с Postgres
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "collector.py"]
