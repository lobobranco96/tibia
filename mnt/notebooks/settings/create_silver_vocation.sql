CREATE TABLE IF NOT EXISTS nessie.silver.vocation (
  name STRING,
  world STRING,
  vocation STRING,
  level INT,
  experience LONG,
  world_type STRING,
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
  'write.delete.mode' = 'copy-on-write')