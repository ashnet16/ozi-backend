# Changelog

All notable changes to ozi-backend are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- The readme identifies ozi-backend as v2 of the archived [Ozi](https://github.com/ashnet16/ozi) project.

### Fixed

- Profile updates (`USER_DATA_ADD`) are published to Kafka instead of failing in the poller and going to the dead-letter topic.

### Removed

- The unused DuckDB setup script (`analytics/init_db.py`). Postgres stores all analytics data.

## [0.1.0] - 2026-11-20

First tagged release.

### Added

- Snapchain gRPC poller that publishes merge events to Kafka as a JSON envelope (`schema_version` `v1`).
- Analytics consumer writing casts, comments and reactions to Postgres.
- Embedding consumer writing casts and comments to Qdrant (`intfloat/multilingual-e5-base`).
- FastAPI service for semantic search and LLM-generated SQL, with Redis caching.
- Local Snapchain devnet (`make snapchain-up`) and settings for a hosted Snapchain endpoint: TLS and an API-key header.
- Developer guide in the readme.
- `CONTRIBUTING.md` with Developer Certificate of Origin sign-off.

### Changed

- Postgres rows and Qdrant points are keyed on the message hash. Snapchain event ids are unique only within a shard. Existing tables are migrated on startup.
- Relicensed from MIT to AGPL-3.0-only. Earlier commits remain available under MIT.

### Fixed

- Message timestamps are converted from the Farcaster epoch (2021-01-01 UTC); they were previously 273 days off.
- Message hashes are stored as hex instead of a scrambled encoding.
- The poller reconnects after a stream error instead of reusing a failed channel.

### Removed

- The AWS EC2 deploy script.
- Leftover Neynar social-graph features.

[Unreleased]: https://github.com/ashnet16/ozi-backend/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ashnet16/ozi-backend/releases/tag/v0.1.0
