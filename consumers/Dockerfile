FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY consumers/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY consumers/ ./

CMD ["python", "comsumer.py"]
