from pyspark.sql import functions as F
from datetime import datetime
from uuid import uuid4 
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class Bronze:
  def __init__(self, spark, date_str):
    self.spark = spark
    self.date_str = date_str

  def vocation(self):
      self.spark.conf.set("spark.sql.catalog.nessie.warehouse", "s3a://bronze/")

      self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze.vocation")

      self.spark.sql("""
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

      today_date = datetime.strptime(self.date_str, "%Y-%m-%d") if self.date_str else datetime.today()
      partition = f"year={today_date.year}/month={today_date.month}/day={today_date.day}"

      path = f"s3a://landing/{partition}/vocation/*.csv"
      logging.info(f"Lendo dados de: {path}")

      df_raw = self.spark.read.csv(path, header=True)
      colunas_esperadas = {"Rank", "Name", "Vocation", "World", "Level", "Points", "WorldType"}
      colunas_faltando = colunas_esperadas - set(df_raw.columns)
      if colunas_faltando:
          logging.error(f"Colunas ausentes no CSV: {colunas_faltando}")
          return

      batch_id = str(uuid4())
      logging.info(f"Gerando batch_id: {batch_id}")

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

      self.spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

      record_count = df_bronze.count()
      if record_count > 0:
          logging.info(f"Inserindo {record_count} registros na Bronze com batch_id {batch_id}...")
          df_bronze.writeTo("nessie.bronze.vocation").append()
      else:
          logging.warning("Nenhum registro encontrado para gravar na Bronze.")
  

  def skills(self):
      self.spark.conf.set("spark.sql.catalog.nessie.warehouse", "s3a://bronze/")

      self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze.skills")

      self.spark.sql("""
      CREATE TABLE IF NOT EXISTS nessie.bronze.skills (
          name STRING,
          vocation STRING,
          world STRING,
          level INT,
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
      """)

      today_date = datetime.strptime(self.date_str, "%Y-%m-%d") if self.date_str else datetime.today()
      partition = f"year={today_date.year}/month={today_date.month}/day={today_date.day}"

      path = f"s3a://landing/{partition}/skills/*.csv"
      logging.info(f"Lendo dados de: {path}")

      df_raw = self.spark.read.csv(path, header=True)
      colunas_esperadas = {'Rank', 'Name', 'Vocation', 'World', 'Level', 'Skill Level', 'category', 'vocation_id'}
      colunas_faltando = colunas_esperadas - set(df_raw.columns)
      if colunas_faltando:
          logging.error(f"Colunas ausentes no CSV: {colunas_faltando}")
          return
          
      batch_id = str(uuid4())
      logging.info(f"Gerando batch_id: {batch_id}")

      df_bronze = (
          df_raw.drop(["Rank", "vocation_id"])
          .withColumnRenamed("Name", "name")
          .withColumnRenamed("Vocation", "vocation")
          .withColumnRenamed("World", "world")
          .withColumnRenamed("Level", "level")
          .withColumnRenamed("Skill Level", "skill_level")
          .withColumn("ingestion_time", F.current_timestamp())
          .withColumn("ingestion_date", F.current_date()) 
          .withColumn("source_system", F.lit("highscore_tibia_page"))
          .withColumn("batch_id", F.lit(batch_id))
          .withColumn("level", F.col("level").cast("int"))
          .withColumn("skill_level", F.col("skill_level").cast("int"))
          .withColumn("vocation", F.trim(F.lower(F.col("vocation"))))
          .withColumn("world", F.trim(F.lower(F.col("world"))))
          .dropDuplicates(["name", "world"])
      )

      self.spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

      record_count = df_bronze.count()
      if record_count > 0:
          logging.info(f"Inserindo {record_count} registros na Bronze com batch_id {batch_id}...")
          df_bronze.writeTo("nessie.bronze.skills").append()
      else:
          logging.warning("Nenhum registro encontrado para gravar na Bronze.")

  def extra(self):
      """
      Job Bronze para consolidar dados de múltiplos arquivos CSV na pasta 'extra/'.
      Normaliza colunas e grava em tabela Iceberg padronizada.
      """
      self.spark.conf.set("spark.sql.catalog.nessie.warehouse", "s3a://bronze/")
      
      self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze.extra")

      self.spark.sql("""
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
      today_date = datetime.strptime(self.date_str, "%Y-%m-%d") if self.date_str else datetime.today()
      partition = f"year={today_date.year}/month={today_date.month}/day={today_date.day}"
      path = f"s3a://landing/{partition}/extra/*.csv"

      logging.info(f"Lendo arquivos CSV de: {path}")

      #  Lê todos os CSVs
      df_raw = (
          self.spark.read.option("header", True)
          .option("inferSchema", True)
          .csv(path)
      )

      if df_raw.head(1) == []:
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

      self.spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

      record_count = df_bronze.count()
      if record_count > 0:
          logging.info(f"Inserindo {record_count} registros na Bronze (extra) com batch_id {batch_id}...")
          df_bronze.writeTo("nessie.bronze.extra").append()
          logging.info("Carga concluída com sucesso na Bronze (extra).")
      else:
          logging.warning("Nenhum registro encontrado para gravar na Bronze Extra.")


