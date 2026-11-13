# Ozi

**Ozi** is a real-time semantic search and analytics engine for decentralized social networks, starting with [Farcaster](https://www.farcaster.xyz).

It streams casts, comments, and reactions from live events, embeds them for semantic retrieval, and stores structured event data for analytic SQL queries—all powered by Kafka, Postgres, Qdrant, and LLMs.
Note: This repository contains only the local development environment for the MVP, run with Docker Compose. You may encounter reliability and availability issues as data scales, due to throughput and latency limitations inherent in the current Python-based implementation. We plan to rewrite several lower-level components in Rust as demand increases to improve performance and reduce latency.


![OziArchitecture](./designs/mvp_current.png)


---

## Architecture Overview

| Component | Description |
|-----------|-------------|
| **Kafka Producers** | Poll and publish Farcaster events into Kafka topics |
| **Kafka Consumers** | Consume events and write structured data into Postgres and Qdrant |
| **Postgres** |  relational and lightweight analytic queries |
| **Vector Database (Qdrant)** | Stores text embeddings for semantic search |
| **Redis** | Caches `/search` responses for an hour |
| **FastAPI Server** | Serves APIs for semantic search and structured analytics |
| **LLM Service (OpenAI)** | Classifies user queries and generates SQL if needed |
| **RAG Layer** | Powers retrieval-augmented generation over event streams |



##  System Guarantees

Ozi's data ingestion and analytics pipeline is designed to prioritize:

---

### 1. High Availability (HA)

- The Kafka analytics consumer is built to **restart automatically** after crashes or restarts.
- Messages are consumed **continuously** and **buffers are flushed** to Postgres on normal exit or crash recovery.
- If the consumer goes down temporarily, it will **reconnect and resume** from the latest Kafka offset without loss.

---

### 2. Reliability

- **At least once processing**: Kafka guarantees messages are delivered at least once to the consumer.
- **Bulk insertion**: Records are inserted in efficient batches for speed and to minimize memory usage.

---

### 3. Eventual Consistency

- Ozi is designed to be **eventually consistent**:  
  All valid Farcaster events (casts, comments, reactions) will eventually appear in Postgres.
- **Small ingestion delays are acceptable** in favor of higher throughput and system resilience.
- Queries reflect **near-real-time data** under normal operation.

---

### 4. System Principles

| Principle | How Ozi Achieves It |
|:---|:---|
| High Availability | Consumer automatically reconnects to Kafka after crashes. |
| Eventual Consistency | Message arrival order is not strictly enforced; data is ingested reliably over time. |
| Efficient Batching | Messages are buffered by type and bulk inserted. |
| Graceful Degradation | If traffic spikes, consumers process with slight delay without dropping data. |
| Minimal Operator Overhead | Self-healing consumer design with automatic reconnection and flush logic. |

---

## Microservices and Components

- **producers/** — Snapchain gRPC poller: transforms events and publishes them to Kafka
- **analytics/** — Kafka consumer writing casts, comments and reactions to Postgres
- **consumers/** — Kafka consumer embedding casts into Qdrant
- **rag/api/** — FastAPI app (`main.py`), vector search service, LLM prompts, SQL validator
- **rag/llm/** — Pluggable LLM layer (OpenAI for now, self-hosted later)

---
## Data Flow

Snapchain node → Poller → Kafka topics (`farcaster.events.add` for casts, `farcaster.events.other` for the rest)
Kafka consumers → Postgres (casts/comments/reactions) + Qdrant (cast embeddings)
FastAPI → /search → LLM classifies → (Semantic search OR SQL generation)
FastAPI → /query → Full analytic query results


## Use Cases & Example Queries

Ozi lets you explore Farcaster activity in real-time whether you're a researcher, builder, or curious user.

**Use Cases (WIP):**
- Query Casts via natural language
- Run custom analytics on Farcaster events
- Build dashboards or agents over social data
- Warpcast-integrated search and summarization

---

### Ask Anything

You can interact with Ozi using natural language or SQL. The LLM will translate your questions into queries against the analytics engine. The UI layer will paginate the results (top 5).

**Try asking:**
- _"Which Farcaster users mentioned “Ethereum” the most this week?"_
- _"What are the top topics being discussed about Solana today?"_
- _"Show me all users who cast about tariffs in the last 7 days."_

**Example SQL generated:**

```sql
SELECT
  fid,
  COUNT(*) AS cast_count
FROM casts
WHERE text ILIKE '%tariff%'
  AND idx_time >= NOW() - INTERVAL '7 days'
GROUP BY fid
ORDER BY cast_count DESC
LIMIT 10;
```

**Example API calls** (replace `8000` with `API_HOST_PORT` if you changed it):

```bash
# 1. Register a user (required once; creates the quota row)
curl -X POST http://localhost:8000/auth \
  -H "Content-Type: application/json" \
  -d '{"fid": "123"}'

# 2. Natural-language search ("general" filters to the user's own activity, "deep" searches everything)
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{"query": "What are people saying about Ethereum?", "fid": "123", "search_mode": "deep"}'

# 3. Run a read-only SQL query directly
curl -G http://localhost:8000/query \
  --data-urlencode "fid=123" \
  --data-urlencode "sql=SELECT fid, COUNT(*) AS cast_count FROM casts GROUP BY fid ORDER BY cast_count DESC LIMIT 10;"
```
---

## Technologies Used

- **[FastAPI](https://fastapi.tiangolo.com/)** — High-performance Python web framework for building APIs
- **[Postgres](https://www.postgresql.org/)** — Database for lightweight analytical queries on structured data
- **[Kafka](https://kafka.apache.org/)** — Distributed event streaming platform for ingesting real-time events
- **[Qdrant](https://qdrant.tech/)** — Vector database for semantic search over cast embeddings
- **[Redis](https://redis.io/)** — Response cache for search queries
- **[Sentence-Transformers](https://www.sbert.net/)** — Text embedding models for semantic similarity search
- **[OpenAI / ChatGPT APIs](https://platform.openai.com/)** — Language model service for query classification and SQL generation
- **[Docker + Docker Compose](https://docs.docker.com/compose/)** — Containerization and local orchestration of services



## API Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/auth`    | Register a user by `fid` and return remaining quotas |
| `POST` | `/search`  | Natural-language search (`query`, `fid`, `search_mode` = `general` or `deep`) |
| `GET`  | `/query`   | Run a validated read-only SQL query (`sql`, `fid`) |
| `GET`  | `/health`  | Health check to verify the API can reach Postgres |
| `GET`  | `/queries` | Returns the most recent queries received by Ozi (default 100) |

There is no web UI; the API is JSON only.


## Getting Started

Requirements: Docker (with Compose), `make` and `git`. No host Python is needed.

#### 1. Clone

```bash
git clone https://github.com/ashnet16/ozi.git
cd ozi
```

#### 2. Configure

```bash
cp .env.example .env
```

Fill in `POSTGRES_USER`, `POSTGRES_PASSWORD` and `OPENAI_API_KEY`. Optionally set:

- `POSTGRES_HOST_PORT`, `REDIS_HOST_PORT`, `API_HOST_PORT` if 5432 / 6379 / 8000 are already in use
- `SNAPCHAIN_ENDPOINT` — gRPC address of the Snapchain node the poller subscribes to (default `host.docker.internal:3383`, i.e. a node running on your machine)

#### 3. Build and run

```bash
make setup
```

This clones the Snapchain repo for its `.proto` files, builds the images (the protos are compiled inside the poller image) and starts everything. The first build and first start are slow: the images include PyTorch and the embedding model is downloaded on startup.

#### 4. Query the API at http://localhost:8000 (see the example calls above)



#### Below are the Kafka Topics

```
kafka-topics --bootstrap-server localhost:9092 \
  --create --topic farcaster.events.add --partitions 1 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 \
  --create --topic farcaster.events.other --partitions 1 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 \
  --create --topic farcaster.events.dlq --partitions 1 --replication-factor 1
```

#### Consume inside the container
```
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic farcaster.events.add \
  --from-beginning \
  --property print.key=true \
  --property print.value=true

```
