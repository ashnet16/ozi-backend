FROM python:3.10-slim

WORKDIR /app/rag

RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

COPY rag/requirements.txt requirements.txt

RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["uvicorn", "rag.api.main:app", "--host", "0.0.0.0", "--port", "8000"]