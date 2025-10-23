from src.utility import create_spark_session
from pyspark.sql.functions import col, regexp_replace, current_timestamp, lower, trim
from datetime import datetime
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def bronze_vocation(spark, date_str=None):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze.vocation")

    spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.bronze.vocation (
        name STRING,
        vocation STRING,
        world STRING,
        level INT,
        experience LONG,
        world_type STRING,
        ingestion_time TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (world, days(ingestion_time))
    """)

    today_date = datetime.strptime(date_str, "%Y-%m-%d") if date_str else datetime.today()
    partition = f"year={today_date.year}/month={today_date.month}/day={today_date.day}"

    path = f"s3a://landing/{partition}/vocation/*.csv"
    logging.info(f"Lendo dados de: {path}")

    df_raw = spark.read.csv(path, header=True)
    expected_cols = {"Name", "Vocation", "World", "Points", "WorldType", "Level"}
    missing_cols = expected_cols - set(df_raw.columns)
    if missing_cols:
        logging.error(f"Colunas ausentes no CSV: {missing_cols}")
        return

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
        .withColumn("ingestion_time", current_timestamp())
        .withColumn("experience", regexp_replace(col("experience"), ",", "").cast("long"))
        .withColumn("level", col("level").cast("int"))
        .withColumn("vocation", trim(lower(col("vocation"))))
        .withColumn("world", trim(lower(col("world"))))
        .dropDuplicates(["name", "world"])
    )

    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

    if df_bronze.head(1):
        record_count = df_bronze.count()
        logging.info(f"Inserindo {record_count} registros na Bronze...")
        df_bronze.writeTo("nessie.bronze.vocation").append()
    else:
        logging.warning("Nenhum registro encontrado para gravar na Bronze.")


if __name__ == "__main__":
    date_str = None
    if "--date" in sys.argv:
        date_str = sys.argv[sys.argv.index("--date") + 1]

    spark = create_spark_session("bronze_vocation")
    bronze_vocation(spark, date_str)
