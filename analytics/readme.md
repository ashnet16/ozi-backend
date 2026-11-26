# Analytics data dictionary

The analytics consumer stores Farcaster activity in three Postgres tables, records removals in a fourth, and exposes the current data through three views. All are created on startup by `ensure_tables_exist` in [consumer.py](consumer.py); the definitions are in [table_queries.py](table_queries.py), which is the authority if this page and the code disagree.

| Relation | What it holds |
| --- | --- |
| `casts`, `comments`, `reactions` | Every cast, reply and like or recast received, including ones later removed. |
| `removed_messages` | One row per removed message: deleted casts and replies, removed reactions. |
| `api.live_casts`, `api.live_comments`, `api.live_reactions` | The three tables without their removed rows. The only relations the API's generated SQL can read. |

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

## `removed_messages`

Removals are recorded here rather than deleted from the tables above, so a row of `casts`, `comments` or `reactions` stays after its message is removed. Rows are keyed on the removed message's hash, so recording the same removal again does nothing, and a removal can be recorded before or without the message it removes.

| Column | Type | Description |
| --- | --- | --- |
| `msg_hash_hex` | TEXT, primary key | Hash of the removed message. Matches `msg_hash_hex` in the table that holds it, if any. |
| `message_type` | TEXT | Type of the removed message: `CAST_ADD`, `REACTION_ADD`, or `REACTION_REMOVE` for a remove undone by a re-added reaction. |
| `fid` | BIGINT | Author of the removed message. |
| `reason` | TEXT | `cast_remove`, `reaction_remove`, or `replaced` when a later add displaced the message. |
| `removed_by_hash_hex` | TEXT | Hash of the message that removed it. |
| `removed_at` | TIMESTAMPTZ | When that message was created (Farcaster time). |
| `idx_time` | TIMESTAMPTZ | When Ozi recorded the removal. |

Which message was removed comes from Snapchain: each event lists the messages its merge displaced, and a cast remove also names its cast. A reaction remove names its reaction by author, type and target, not by hash, so when Snapchain displaced nothing (an unlike of a like that never existed, or a remove for a recast where only a like exists), the removed hash is unknown. That remove is recorded under its own hash, with `removed_by_hash_hex` the same, and hides nothing.

## Views: `api.live_casts`, `api.live_comments`, `api.live_reactions`

Each view is its table with every row whose `msg_hash_hex` is in `removed_messages` left out, so they have the same columns as the tables. Deleted casts and replies do not appear, and like and recast counts include only reactions that were not removed. A like that was removed and then added again is counted once, under the re-add's hash.

The views are re-created on every start of the analytics consumer, after any table change, so they pick up added columns.

The API runs generated SQL as a separate read-only role, `ozi_query` (`OZI_QUERY_USER`), whose search path is the `api` schema: it can read these views by their bare names and cannot read the tables in `public`. The analytics consumer creates the role and sets its password from `OZI_QUERY_PASSWORD`.

## Not stored

Follows, unfollows, profile data, and pruned or revoked messages are not stored yet.
