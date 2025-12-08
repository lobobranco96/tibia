from datetime import datetime
from uuid import uuid4 
import logging
import sys
from pyspark.files import SparkFiles
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class Silver:
  def __init__(self, spark):
    self.spark = spark

  def vocation(self):
      try:
          self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver.vocation")
          self.spark.sql("""
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
          TBLPROPERTIES (
              'format-version' = '2',
              'write.format.default' = 'parquet',
              'write.metadata.compression' = 'gzip',
              'write.delete.mode' = 'merge-on-read'
          )
          """)

          logging.info("Tabela Silver inicializada com sucesso.")

          df_bronze = self.spark.read.table("nessie.bronze.vocation")

          if df_bronze.head(1) == []:
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

          logging.info("Aplicando MERGE INTO (SCD Type 2) na Silver...")

          merge_query = """
          MERGE INTO nessie.silver.vocation AS target
          USING vocation_updates AS source
          ON target.name = source.name AND target.is_current = TRUE

          WHEN MATCHED AND (
              target.level <> source.level OR
              target.experience <> source.experience OR
              target.vocation <> source.vocation OR
              target.world <> source.world OR
              target.world_type <> source.world_type
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
          """

          self.spark.sql(merge_query)
          logging.info("MERGE INTO finalizado com sucesso!")

          # Auditoria: registros totais, atuais, updates/inserts
          df_check = self.spark.read.table("nessie.silver.vocation")
          total_rows = df_check.count()
          current_rows = df_check.filter("is_current = true").count()
          logging.info(f"Total de registros na Silver: {total_rows}")
          logging.info(f"Registros atuais (is_current = true): {current_rows}")

      except Exception as e:
          logging.exception(f"Falha no job Silver: {str(e)}")
          sys.exit(1)

  def skills(self):
      try:
          self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver.skills")

          self.spark.sql("""
          CREATE TABLE IF NOT EXISTS nessie.silver.skills (
              name STRING,
              vocation STRING,
              world STRING,
              level INT,
              skill_level INT,
              category STRING,
              ingestion_time TIMESTAMP,
              start_date TIMESTAMP,
              end_date TIMESTAMP,
              is_current BOOLEAN
          )
          USING iceberg
          PARTITIONED BY (world, days(start_date), bucket(8, name))
          TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.metadata.compression' = 'gzip',
            'write.delete.mode' = 'merge-on-read'
          )
          """)

          logging.info("Tabela Silver 'skills' inicializada com sucesso.")

          df_bronze = self.read.table("nessie.bronze.skills")

          if df_bronze.head(1) == []:
              logging.warning("Nenhum dado encontrado na Bronze. Encerrando execução.")
              return

          df_new = (
              df_bronze
              .withColumn("start_date", F.current_timestamp())
              .withColumn("end_date", F.lit(None).cast("timestamp"))
              .withColumn("is_current", F.lit(True))
          )

          df_new.createOrReplaceTempView("skills_updates")
          logging.info("View temporária 'skills_updates' criada com sucesso.")

          logging.info("Aplicando MERGE INTO (SCD Type 2) na Silver...")

          merge_query = """
          MERGE INTO nessie.silver.skills AS target
          USING skills_updates AS source
          ON target.name = source.name
            AND target.category = source.category
            AND target.is_current = TRUE

          WHEN MATCHED AND (
              target.level <> source.level OR
              target.skill_level <> source.skill_level OR
              target.vocation <> source.vocation OR
              target.world <> source.world
          ) THEN
            UPDATE SET
              target.end_date = current_timestamp(),
              target.is_current = FALSE

          WHEN NOT MATCHED BY TARGET THEN
            INSERT (
              name, vocation, world, level, skill_level, category,
              ingestion_time, start_date, end_date, is_current
            )
            VALUES (
              source.name, source.vocation, source.world,
              source.level, source.skill_level, source.category,
              source.ingestion_time, current_timestamp(), NULL, TRUE
            )
          """

          self.spark.sql(merge_query)
          logging.info("MERGE INTO concluído com sucesso!")

          df_check =self. spark.read.table("nessie.silver.skills")
          total_rows = df_check.count()
          current_rows = df_check.filter("is_current = true").count()

          logging.info(f"Total de registros na Silver: {total_rows}")
          logging.info(f"Registros atuais (is_current = true): {current_rows}")

      except Exception as e:
          logging.exception(f"Falha no job Silver (skills): {str(e)}")
          sys.exit(1)

