CREATE TABLE IF NOT EXISTS nessie.gold.experience_global_rank (
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
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
    'format-version' = '2'
);;