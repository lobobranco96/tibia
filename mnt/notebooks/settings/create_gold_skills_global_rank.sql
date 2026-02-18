CREATE TABLE IF NOT EXISTS nessie.gold.skills_global_rank (
    name STRING,
    world STRING,
    skill_name STRING,
    vocation STRING,
    skill_level INT,
    updated_at TIMESTAMP,
    snapshot_date DATE
)
USING iceberg
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
    'format-version' = '2'
);