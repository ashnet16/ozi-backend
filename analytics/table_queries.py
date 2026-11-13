def create_casts_table():
    return """
    CREATE TABLE IF NOT EXISTS casts (
        id BIGINT PRIMARY KEY,
        msg_type TEXT,
        fid BIGINT,
        msg_timestamp TIMESTAMP WITH TIME ZONE,
        text TEXT,
        lang TEXT,
        embeds TEXT,
        mentions TEXT,
        msg_hash TEXT,
        msg_hash_hex TEXT,
        idx_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_casts_msg_hash_hex ON casts (msg_hash_hex);
    """


def create_comments_table():
    return """
    CREATE TABLE IF NOT EXISTS comments (
        id BIGINT PRIMARY KEY,
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
        msg_hash_hex TEXT,
        idx_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_comments_parent_hash_hex ON comments (parent_hash_hex);
    CREATE INDEX IF NOT EXISTS idx_comments_msg_hash_hex ON comments (msg_hash_hex);
    """


def create_reactions_table():
    return """
    CREATE TABLE IF NOT EXISTS reactions (
        id BIGINT PRIMARY KEY,
        msg_type TEXT,
        fid BIGINT,
        msg_timestamp TIMESTAMP WITH TIME ZONE,
        reaction_type TEXT,
        target_fid BIGINT,
        target_hash TEXT,
        target_hash_hex TEXT,
        msg_hash TEXT,
        msg_hash_hex TEXT,
        idx_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_reactions_target_hash_hex ON reactions (target_hash_hex);
    CREATE INDEX IF NOT EXISTS idx_reactions_msg_hash_hex ON reactions (msg_hash_hex);
    """
