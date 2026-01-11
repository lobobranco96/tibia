CREATE TABLE IF NOT EXISTS nessie.silver.skills (
    name STRING,
    world STRING,
    category STRING,
    vocation STRING,
    skill_level INT,
    ingestion_time TIMESTAMP,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN,
    hash_diff STRING
)
USING iceberg
PARTITIONED BY (world)
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write'
    )