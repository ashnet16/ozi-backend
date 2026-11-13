FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY analytics/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

COPY analytics/ /app/
COPY consumers/ /app/consumers/

ENV PYTHONPATH=/app

CMD ["python", "consumer.py"]
