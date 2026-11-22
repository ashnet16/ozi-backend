# Analytics data dictionary

The analytics consumer stores Farcaster activity in three Postgres tables. They are created on startup by `ensure_tables_exist` in [consumer.py](consumer.py); the definitions are in [table_queries.py](table_queries.py), which is the authority if this page and the code disagree.

## Relationships

```
comments.parent_hash_hex   ──►  casts.msg_hash_hex  or  comments.msg_hash_hex
reactions.target_hash_hex  ──►  casts.msg_hash_hex  or  comments.msg_hash_hex
```

- Every row is keyed on `msg_hash_hex`, the hash of the Farcaster message that created it.
- A comment's `parent_hash_hex` is the hash of the cast or comment it replies to, so it matches `casts.msg_hash_hex` or `comments.msg_hash_hex`.
- A reaction's `target_hash_hex` is the hash of the cast or comment it reacts to, in either table.
- The target of a comment or reaction may not be stored, for example when it was posted before Ozi started collecting.

## Columns shared by all three tables

| Column | Type | Description |
| --- | --- | --- |
| `msg_hash_hex` | TEXT, primary key | Message hash, 40 lowercase hex characters. Unique per row. |
| `msg_hash` | TEXT | The same hash as received in the event envelope. Currently identical to `msg_hash_hex`. |
| `id` | BIGINT | Snapchain event id. Unique only within a shard, so not unique here; do not count or join on it. |
| `msg_type` | TEXT | `CAST_ADD` for casts and comments, `REACTION_ADD` for reactions. |
| `fid` | BIGINT | Farcaster id of the author. |
| `msg_timestamp` | TIMESTAMPTZ | When the message was created, converted from Farcaster time (seconds since 2021-01-01 UTC). |
| `idx_time` | TIMESTAMPTZ | When Ozi stored the row. Replaying or rebuilding resets it, so use `msg_timestamp` for when something was posted. |

## `casts`

Top-level casts: casts that are not replies.

| Column | Type | Description |
| --- | --- | --- |
| `text` | TEXT | Cast text. |
| `lang` | TEXT | Language detected by `langdetect`. Unreliable for short text; `NULL` when the text is three characters or fewer. |
| `embeds` | TEXT | JSON list of embedded URLs, for example `["https://…"]`. |
| `mentions` | TEXT | JSON list of mentioned fids. |

## `comments`

Replies: casts with a parent cast.

| Column | Type | Description |
| --- | --- | --- |
| `parent_fid` | BIGINT | Author of the cast being replied to. |
| `parent_hash_hex` | TEXT | Hash of the cast being replied to, in hex. Use this for joins. |
| `parent_hash` | TEXT | The same hash in base64, as it appears in the raw event. |
| `text` | TEXT | Reply text. |
| `lang` | TEXT | Detected language, as for casts. |
| `embeds` | TEXT | JSON list of embedded URLs. |

Comments have no `mentions` column.

## `reactions`

Likes and recasts.

| Column | Type | Description |
| --- | --- | --- |
| `reaction_type` | TEXT | `REACTION_TYPE_LIKE` or `REACTION_TYPE_RECAST`. |
| `target_fid` | BIGINT | Author of the cast reacted to. |
| `target_hash_hex` | TEXT | Hash of the cast reacted to, in hex. Use this for joins. |
| `target_hash` | TEXT | The same hash as received in the event envelope. Currently identical to `target_hash_hex`. |

A reaction to a URL instead of a cast is currently stored with `target_fid` 0 and an empty target hash.

## Not stored

Removals (deleted casts, unlikes), follows and profile data are not stored yet.
