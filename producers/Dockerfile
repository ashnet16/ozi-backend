FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY producers/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY producers/ ./
COPY proto/*.proto ./proto/
RUN python -m grpc_tools.protoc -I./proto --python_out=./proto --grpc_python_out=./proto ./proto/*.proto

# Generated modules import each other by bare name (e.g. `import message_pb2`).
ENV PYTHONPATH=/app/proto

CMD ["python", "poller.py"]

