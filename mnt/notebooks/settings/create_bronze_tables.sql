# Criação das tabelas bronze

CREATE TABLE IF NOT EXISTS nessie.bronze.vocation (
    name STRING,
    vocation STRING,
    level INT,
    world STRING,
    experience LONG,
    world_type STRING,
    ingestion_time TIMESTAMP,
    ingestion_date DATE,
    source_system STRING,
    batch_id STRING
)
USING iceberg
PARTITIONED BY (world, ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.metadata.compression' = 'gzip',
    'write.delete.mode' = 'merge-on-read'
)
CREATE TABLE IF NOT EXISTS nessie.bronze.skills (
  name STRING,
  vocation STRING,
  world STRING,
  skill_level INT,
  category STRING,
  ingestion_time TIMESTAMP,
  ingestion_date DATE,
  source_system STRING,
  batch_id STRING
)
USING iceberg
PARTITIONED BY (world, ingestion_date)
TBLPROPERTIES (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'write.metadata.compression' = 'gzip',
  'write.delete.mode' = 'merge-on-read'
)

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