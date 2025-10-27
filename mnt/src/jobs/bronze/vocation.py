from src.jobs.utility import create_spark_session
from pyspark.sql import functions as F
from datetime import datetime
from uuid import uuid4  # ✅ Import necessário para gerar batch_id único
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def bronze_vocation(spark, date_str=None):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze.vocation")

    spark.sql("""
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
    """)

    today_date = datetime.strptime(date_str, "%Y-%m-%d") if date_str else datetime.today()
    partition = f"year={today_date.year}/month={today_date.month}/day={today_date.day}"

    path = f"s3a://landing/{partition}/vocation/*.csv"
    logging.info(f"Lendo dados de: {path}")

    df_raw = spark.read.csv(path, header=True)
    colunas_esperadas = {"Rank", "Name", "Vocation", "World", "Level", "Points", "WorldType"}
    colunas_faltando = colunas_esperadas - set(df_raw.columns)
    if colunas_faltando:
        logging.error(f"Colunas ausentes no CSV: {colunas_faltando}")
        return

    batch_id = str(uuid4())
    logging.info(f"Gerando batch_id: {batch_id}")

    # Log de schema (opcional)
    df_raw.printSchema()

    df_bronze = (
        df_raw.drop("Rank")
        .withColumnRenamed("Name", "name")
        .withColumnRenamed("Vocation", "vocation")
        .withColumnRenamed("Level", "level")
        .withColumnRenamed("World", "world")
        .withColumnRenamed("Points", "experience")
        .withColumnRenamed("WorldType", "world_type")
        .withColumn("ingestion_time", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
        .withColumn("source_system", F.lit("highscore_tibia_page"))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("experience", F.regexp_replace(F.col("experience"), ",", "").cast("long"))
        .withColumn("level", F.col("level").cast("int"))
        .withColumn("vocation", F.trim(F.lower(F.col("vocation"))))
        .withColumn("world", F.trim(F.lower(F.col("world"))))
        .dropDuplicates(["name", "world"])
    )

    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

    record_count = df_bronze.count()
    if record_count > 0:
        logging.info(f"Inserindo {record_count} registros na Bronze com batch_id {batch_id}...")
        df_bronze.writeTo("nessie.bronze.vocation").append()
    else:
        logging.warning("Nenhum registro encontrado para gravar na Bronze.")


if __name__ == "__main__":
    date_str = None
    if "--date" in sys.argv:
        date_str = sys.argv[sys.argv.index("--date") + 1]

    try:
        spark = create_spark_session("bronze_vocation")
        bronze_vocation(spark, date_str)
    except Exception as e:
        logging.exception(f"Falha no job Bronze: {str(e)}")
        sys.exit(1)
