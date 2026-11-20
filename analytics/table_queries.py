# Rows are keyed on the message hash. `id` is the Snapchain event id, which is
# only unique within a shard, so it cannot be the key.

TABLES = ("casts", "comments", "reactions")

# Days between the Farcaster epoch (2021-01-01) and the 2021-10-01 date that
# older versions used by mistake when converting timestamps.
_OLD_EPOCH_ERROR_DAYS = 273


def primary_key_column():
    """Query returning the primary-key column of the table passed as a parameter."""
    return """
    SELECT a.attname
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = %s::regclass AND i.indisprimary;
    """


def migrate_to_hash_key(table):
    """One-off repair of a table created when `id` was the primary key.

    Older versions also base64-decoded hashes that were already hex and used the
    wrong epoch, so this rewrites the hex columns and shifts the timestamps before
    moving the key. Run it only while the key is still `id`.
    """
    assert table in TABLES
    hex_repairs = [("msg_hash_hex", "msg_hash")]
    if table == "reactions":
        hex_repairs.append(("target_hash_hex", "target_hash"))
    statements = [
        f"UPDATE {table} SET {dst} = lower({src}) WHERE {src} ~* '^[0-9a-f]{{40}}$';"
        for dst, src in hex_repairs
    ]
    statements += [
        f"UPDATE {table} SET msg_timestamp = msg_timestamp"
        f" - INTERVAL '{_OLD_EPOCH_ERROR_DAYS} days';",
        f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {table}_pkey;",
        f"ALTER TABLE {table} ADD PRIMARY KEY (msg_hash_hex);",
    ]
    return "BEGIN;\n" + "\n".join(statements) + "\nCOMMIT;"


def create_casts_table():
    return """
    CREATE TABLE IF NOT EXISTS casts (
        id BIGINT,
        msg_type TEXT,
        fid BIGINT,
        msg_timestamp TIMESTAMP WITH TIME ZONE,
        text TEXT,
        lang TEXT,
        embeds TEXT,
        mentions TEXT,
        msg_hash TEXT,
        msg_hash_hex TEXT PRIMARY KEY,
        idx_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_casts_msg_hash_hex ON casts (msg_hash_hex);
    """


def create_comments_table():
    return """
    CREATE TABLE IF NOT EXISTS comments (
        id BIGINT,
        msg_type TEXT,
        fid BIGINT,
        msg_timestamp TIMESTAMP WITH TIME ZONE,
        parent_fid BIGINT,
        parent_hash TEXT,
        parent_hash_hex TEXT,
        text TEXT,
        lang TEXT,
        embeds TEXT,
        msg_hash TEXT,
        msg_hash_hex TEXT PRIMARY KEY,
        idx_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_comments_parent_hash_hex ON comments (parent_hash_hex);
    CREATE INDEX IF NOT EXISTS idx_comments_msg_hash_hex ON comments (msg_hash_hex);
    """


def create_reactions_table():
    return """
    CREATE TABLE IF NOT EXISTS reactions (
        id BIGINT,
        msg_type TEXT,
        fid BIGINT,
        msg_timestamp TIMESTAMP WITH TIME ZONE,
        reaction_type TEXT,
        target_fid BIGINT,
        target_hash TEXT,
        target_hash_hex TEXT,
        msg_hash TEXT,
        msg_hash_hex TEXT PRIMARY KEY,
        idx_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_reactions_target_hash_hex ON reactions (target_hash_hex);
    CREATE INDEX IF NOT EXISTS idx_reactions_msg_hash_hex ON reactions (msg_hash_hex);
    """
