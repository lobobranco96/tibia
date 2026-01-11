CREATE TABLE IF NOT EXISTS nessie.bronze.extra (
    name STRING,
    vocation STRING,
    world STRING,
    category STRING,
    title STRING,
    points INT,
    ingestion_time TIMESTAMP,
    ingestion_date DATE,
    source_system STRING,
    batch_id STRING,
    source_file STRING
)
USING iceberg
PARTITIONED BY (world, ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.metadata.compression' = 'gzip',
    'write.delete.mode' = 'merge-on-read'
)