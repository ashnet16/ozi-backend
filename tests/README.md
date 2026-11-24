# Tests

```bash
make test                      # everything
make test ARGS="-k search -x"  # pass options to pytest
```

`make test` needs [uv](https://docs.astral.sh/uv/), not Docker or GitHub access. The
first run creates `.venv` (Python 3.11), installs `tests/requirements.txt` from PyPI,
and compiles the checked-in Snapchain protos in `./proto`; later runs reuse them.

## What runs for real

| Piece | In the tests |
| --- | --- |
| Poller (`SnapchainGRPCPoller.run`, transformer, topic routing) | real; the gRPC stream is fed from `analytics/message_sample.json` |
| Kafka | in-memory broker (`fakes.FakeKafka`), one ordered log |
| Analytics consumer → Postgres | real consumer, real Postgres 16 (embedded with `pgserver`) |
| Semantic consumer → Qdrant | real consumer, real Qdrant client in `:memory:` mode |
| Embedding model | `fakes.FakeSentenceTransformer`, a bag-of-words embedder |
| FastAPI app (`/search`, `/auth`, `/query`, `/health`) | real, including startup and its SQL |
| OpenAI, Redis | `fakes.FakeLLM`, `fakes.FakeRedis` |

To use your own Postgres instead of the embedded one, set
`OZI_TEST_POSTGRES_URL=postgresql://user:pass@host:5432/dbname`. Every test drops
and re-creates that database's `public` schema, so point it at a scratch database.

## Files

- `test_routing.py`: which Kafka topic each Snapchain message goes to, and its payload.
- `test_pipeline.py`: events through the poller and both consumers into Postgres and Qdrant.
- `test_search.py`: `/search` (analytic, semantic, both), caching, quota, logging, over piped-in data.
- `samples.py`: loads the sample events and derives expected results straight from the JSON.
- `fakes.py`, `conftest.py`: the stand-ins and fixtures above.

Tests marked `xfail` document existing bugs. They are strict, so fixing a bug makes
the run fail until the marker is removed.
