CREATE TABLE IF NOT EXISTS nessie.gold.experience_rank_global (
    rank INT,
    name STRING,
    world STRING,
    vocation STRING,
    level INT,
    experience BIGINT,
    world_type STRING,
    updated_at TIMESTAMP,
    snapshot_date DATE
)
USING iceberg