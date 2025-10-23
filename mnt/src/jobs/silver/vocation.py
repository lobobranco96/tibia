from src.utility import create_spark_session
from pyspark.sql import functions as F
from datetime import datetime
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def silver_vocation_scd2(spark, date_str=None):

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver.vocation")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.silver.vocation (
        name STRING,
        vocation STRING,
        world STRING,
        level INT,
        experience LONG,
        world_type STRING,
        ingestion_time TIMESTAMP,
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        is_current BOOLEAN
    )
    USING iceberg
    PARTITIONED BY (world, days(start_date), bucket(8, name))
    """)

    logging.info("Tabela Silver inicializada com sucesso.")

    # Ler dados da Bronze
    today_date = datetime.strptime(date_str, "%Y-%m-%d") if date_str else datetime.today()
    partition = f"year={today_date.year}/month={today_date.month}/day={today_date.day}"

    logging.info(f"Lendo dados da Bronze para a partição: {partition}")

    df_bronze = spark.read.table("nessie.bronze.vocation")

    if df_bronze.rdd.isEmpty():
        logging.warning("Nenhum dado encontrado na Bronze. Encerrando execução.")
        return

    df_new = (
        df_bronze
        .withColumn("start_date", F.current_timestamp())
        .withColumn("end_date", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    df_new.createOrReplaceTempView("vocation_updates")

    logging.info("View temporária `vocation_updates` criada com sucesso.")

    # Aplicar SCD Type 2 com Iceberg MERGE INTO
    logging.info("Aplicando MERGE INTO (SCD Type 2) na Silver...")

    spark.sql("""
    MERGE INTO nessie.silver.vocation AS target
    USING vocation_updates AS source
    ON target.name = source.name AND target.is_current = TRUE

    WHEN MATCHED AND (
        target.level <> source.level OR
        target.experience <> source.experience OR
        target.vocation <> source.vocation OR
        target.world <> source.world
    ) THEN
      UPDATE SET
        target.end_date = current_timestamp(),
        target.is_current = FALSE

    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        name, vocation, world, level, experience, world_type,
        ingestion_time, start_date, end_date, is_current
      )
      VALUES (
        source.name, source.vocation, source.world, source.level, source.experience,
        source.world_type, source.ingestion_time, current_timestamp(), NULL, TRUE
      )
    """)

    logging.info("MERGE INTO finalizado com sucesso!")

    # Validação final
    df_check = spark.read.table("nessie.silver.vocation")
    total_rows = df_check.count()
    current_rows = df_check.filter("is_current = true").count()

    logging.info(f"Total de registros na Silver: {total_rows}")
    logging.info(f"Registros atuais (is_current = true): {current_rows}")

    logging.info("Pipeline Silver concluído com sucesso")

if __name__ == "__main__":
    date_str = None
    if "--date" in sys.argv:
        date_str = sys.argv[sys.argv.index("--date") + 1]

    spark = create_spark_session("silver_vocation_scd2")
    silver_vocation_scd2(spark, date_str)
