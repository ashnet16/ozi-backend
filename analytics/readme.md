### Ozi Farcaster Data Dictionary

## Entity-Relationship Diagram

                         +----------------+
                         |      casts      |
                         +----------------+
                         | id (PK)         |
                         | msg_type        |
                         | fid             |
                         | msg_timestamp   |
                         | network         |
                         | parent_fid      |
                         | parent_hash     |
                         | text            |
                         | hash            |
                         | signature       |
                         | signer          |
                         +----------------+
                                 ▲
                                 |
                +----------------+----------------+
                |                                 |
        +---------------+                +----------------+
        |    comments    |                |    reactions    |
        +---------------+                +----------------+
        | id (PK)        |                | id (PK)         |
        | msg_type       |                | msg_type        |
        | fid            |                | fid             |
        | msg_timestamp  |                | msg_timestamp   |
        | network        |                | network         |
        | parent_fid (*) |                | reaction_type   |
        | parent_hash(*) |                | target_fid      |
        | text           |                | target_hash     |
        | hash           |                | hash            |
        | signature      |                | signature       |
        | signer         |                | signer          |
        +---------------+                +----------------+

(*) In `comments`, `parent_fid` and `parent_hash` point to a cast
(*) In `reactions`, `target_fid` and `target_hash` point to a cast or comment


## Cast Table

| Column       | Type            | Description |
|--------------|-----------------|-------------|
| id           | BIGINT           | Event ID (from `event[id]`) |
| msg_type     | TEXT             | Message type (should be `MESSAGE_TYPE_CAST_ADD`) |
| fid          | BIGINT           | Farcaster ID (from `message[data][fid]`) |
| msg_timestamp | TIMESTAMP       | Message timestamp (from `message[data][timestamp]`) |
| network      | TEXT             | Farcaster network (e.g., `FARCASTER_NETWORK_MAINNET`) |
| parent_fid   | BIGINT (nullable)| Parent cast FID (if replying to another cast) |
| parent_hash  | TEXT (nullable)  | Parent cast hash (if replying to another cast) |
| text         | TEXT             | Text content of the cast |




## Comments Table

| Column       | Type            | Description |
|--------------|-----------------|-------------|
| id           | BIGINT           | Event ID (from `event[id]`) |
| msg_type     | TEXT             | Message type (should be `MESSAGE_TYPE_CAST_ADD`) |
| fid          | BIGINT           | Farcaster ID (from `message[data][fid]`) |
| msg_timestamp | TIMESTAMP       | Message timestamp (from `message[data][timestamp]`) |
| network      | TEXT             | Farcaster network |
| parent_fid   | BIGINT           | Parent cast FID (comment is replying to this) |
| parent_hash  | TEXT             | Parent cast hash |
| text         | TEXT             | Text content of the comment |
| hash         | TEXT             | Message hash |
| signature    | TEXT             | Message signature |
| signer       | TEXT             | Message signer public key |






## Reactions Table

| Column       | Type            | Description |
|--------------|-----------------|-------------|
| id           | BIGINT           | Event ID (from `event[id]`) |
| msg_type     | TEXT             | Message type (should be `MESSAGE_TYPE_CAST_ADD`) |
| fid          | BIGINT           | Farcaster ID (from `message[data][fid]`) |
| msg_timestamp | TIMESTAMP       | Message timestamp (from `message[data][timestamp]`) |
| network      | TEXT             | Farcaster network |
| parent_fid   | BIGINT           | Parent cast FID (comment is replying to this) |
| parent_hash  | TEXT             | Parent cast hash |
| text         | TEXT             | Text content of the comment |
| hash         | TEXT             | Message hash |
| signature    | TEXT             | Message signature |
| signer       | TEXT             | Message signer public key |





## Social Graph

Coming Soon
