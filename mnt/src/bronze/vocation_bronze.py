from spark.session import create_spark_session
from pyspark.sql.functions import col, regexp_replace, current_timestamp
import sys
import logging

def bronze_vocation(spark, partition):

  spark.sql("""
  CREATE NAMESPACE IF NOT EXISTS nessie.bronze.vocation
  """)

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
  PARTITIONED BY (world)
  """)

  df_raw = spark.read.csv(f"s3a://landing/{partition}/vocation/*.csv", header=True)

  df_bronze = (
      df_raw
      .drop("Rank") \
      .withColumnRenamed("Name", "name") \
      .withColumnRenamed("Vocation", "vocation") \
      .withColumnRenamed("Level", "level") \
      .withColumnRenamed("Points", "experience") \
      .withColumnRenamed("WorldType", "world_type") \
      .withColumn("ingestion_time", current_timestamp())
      .withColumn("experience", regexp_replace(col("experience"), ",", "").cast("long"))
      .withColumn("level", col("level").cast("int"))
      )

  if df_bronze.count() > 0:
    df_bronze.writeTo("nessie.bronze.vocation").append()
  else:
      logging.warning("Nenhum registro encontrado para gravar na Bronze.")


if "__name__" == "__main__":
  spark = create_spark_session("bronze_vocation")
  partition = sys.args[1] # argumento 0 é o nome do script
  bronze_vocation(spark, partition)