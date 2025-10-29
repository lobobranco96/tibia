from src.jobs.utility import create_spark_session
from pyspark.sql import functions as F
from datetime import datetime
from uuid import uuid4
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def bronze_extra(spark, date_str=None):
    """
    Job Bronze para consolidar dados de múltiplos arquivos CSV na pasta 'extra/'.
    Normaliza colunas e grava em tabela Iceberg padronizada.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze.extra")

    spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.bronze.extra (
        name STRING,
        vocation STRING,
        level INT,
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
    """)

    # Data via argumento 
    today_date = datetime.strptime(date_str, "%Y-%m-%d") if date_str else datetime.today()
    partition = f"year={today_date.year}/month={today_date.month}/day={today_date.day}"
    path = f"s3a://landing/{partition}/extra/*.csv"

    logging.info(f"Lendo arquivos CSV de: {path}")

    #  Lê todos os CSVs
    df_raw = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )

    if df_raw.rdd.isEmpty():
        logging.warning("Nenhum arquivo encontrado em extra/. Encerrando job Bronze Extra.")
        return

    # Normaliza nomes de colunas
    df_raw = df_raw.toDF(*[c.lower().replace(" ", "_") for c in df_raw.columns])

    # Corrige nomes diferentes de colunas
    rename_map = {
        "score": "points",
        "skill_level": "points"
    }
    for old, new in rename_map.items():
        if old in df_raw.columns and new not in df_raw.columns:
            df_raw = df_raw.withColumnRenamed(old, new)

    final_columns = ["name", "vocation", "level", "world", "category", "title", "points"]

    for col in final_columns:
        if col not in df_raw.columns:
            df_raw = df_raw.withColumn(col, F.lit(None))

    df_bronze = df_raw.select(*final_columns)

    batch_id = str(uuid4())
    df_bronze = (
        df_bronze
        .withColumn("ingestion_time", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
        .withColumn("source_system", F.lit("highscore_tibia_page"))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_file", F.input_file_name())
        .withColumn("level", F.col("level").cast("int"))
        .withColumn("points", F.col("points").cast("int"))
        .withColumn("vocation", F.trim(F.lower(F.col("vocation"))))
        .withColumn("world", F.trim(F.lower(F.col("world"))))
        .dropDuplicates(["name", "world", "category"])
    )

    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

    record_count = df_bronze.count()
    if record_count > 0:
        logging.info(f"Inserindo {record_count} registros na Bronze (extra) com batch_id {batch_id}...")
        df_bronze.writeTo("nessie.bronze.extra").append()
        logging.info("Carga concluída com sucesso na Bronze (extra).")
    else:
        logging.warning("Nenhum registro encontrado para gravar na Bronze Extra.")


if __name__ == "__main__":
    date_str = None
    if "--date" in sys.argv:
        date_str = sys.argv[sys.argv.index("--date") + 1]

    try:
        spark = create_spark_session("bronze_extra")
        bronze_extra(spark, date_str)
    except Exception as e:
        logging.exception(f"Falha no job Bronze Extra: {str(e)}")
        sys.exit(1)
