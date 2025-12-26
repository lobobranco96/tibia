"""
Módulo Bronze - Lakehouse Tibia Highscores
------------------------------------------

Este módulo contém a classe `Bronze`, responsável por executar a camada Bronze da
arquitetura Medallion For Data (Landing → Bronze → Silver → Gold).

A camada Bronze possui como objetivo:

- Ler arquivos brutos provenientes da camada Landing (MinIO/S3).
- Validar colunas obrigatórias.
- Aplicar normalizações leves (renomear, normalizar textos, tipos, limpeza).
- Criar tabelas Iceberg versionadas no Nessie.
- Gerar metadados de auditoria (batch_id, ingestion_time, ingestion_date).
- Garantir padronização e deduplicação.
- Escrever dados de forma incremental nas tabelas Bronze.

Os métodos disponíveis correspondem aos conjuntos de dados originais da homepage do Tibia:
- `vocation()` → Tabela de níveis por vocação
- `skills()` → Tabela de skills por jogador
- `extra()` → Tabela com informações complementares (títulos, achievements etc.)

Este módulo é executado via SparkSubmitOperator dentro de uma pipeline Airflow.
"""

from pyspark.sql import functions as F
from datetime import datetime
from uuid import uuid4
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class Bronze:
    """
    Classe responsável pela camada Bronze do Lakehouse Tibia.

    A classe realiza:
    - Configuração do catálogo Nessie + Iceberg.
    - Criação dos namespaces e tabelas caso não existam.
    - Leitura dos arquivos CSV da camada Landing.
    - Padronização dos dados.
    - Registro de metadados operacionais.
    - Escrita incremental via Iceberg (append).

    Parameters
    ----------
    spark : SparkSession
        Sessão Spark ativa criada no job.
    date_str : str | None
        Data fornecida via CLI no formato 'YYYY-MM-DD'.
        Caso None, utiliza a data atual para construir o path da camada Landing.
    """

    def __init__(self, spark, date_str):
        self.spark = spark
        self.date_str = date_str

    #   MÉTODO: VOCATION
    def vocation(self):
        """
        Executa o job Bronze para dados de vocação (vocation).

        1. Configura catálogo Iceberg + warehouse.
        2. Cria namespace + tabela caso não existam.
        3. Lê arquivos CSV da camada landing.
        4. Valida colunas obrigatórias.
        5. Normaliza nomes e tipos de colunas.
        6. Gera batch_id e colunas de auditoria.
        7. Deduplica por (name, world).
        8. Insere incrementalmente na tabela Bronze.

        Tabela criada: nessie.bronze.vocation
        """

        # Namespace
        self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

        # Criação da tabela Iceberg
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

        # Define path da landing com base na data
        today_date = datetime.strptime(self.date_str, "%Y-%m-%d") if self.date_str else datetime.today()
        partition = f"year={today_date.year}/month={today_date.month}/day={today_date.day}"

        path = f"s3a://lakehouse/landing/{partition}/experience/"
        logging.info(f"Lendo dados de: {path}")

        # Lê arquivo CSV
        df_raw = self.spark.read.csv(path, header=True)

        # Validação de colunas esperadas
        colunas_esperadas = {"Rank", "Name", "Vocation", "World", "Level", "Points", "WorldType"}
        colunas_faltando = colunas_esperadas - set(df_raw.columns)

        if colunas_faltando:
            logging.error(f"Colunas ausentes no CSV: {colunas_faltando}")
            return

        # Gera batch_id para auditoria
        batch_id = str(uuid4())
        logging.info(f"Gerando batch_id: {batch_id}")

        df_raw.printSchema()

        # Normalização e padronização
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
            .withColumn("experience", F.regexp_replace("experience", ",", "").cast("long"))
            .withColumn("level", F.col("level").cast("int"))
            .withColumn("vocation", F.trim(F.lower("vocation")))
            .withColumn("world", F.trim(F.lower("world")))
            .dropDuplicates(["name", "world"])
        )

        # Compressão padrão Parquet
        self.spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

        record_count = df_bronze.count()

        if record_count > 0:
            logging.info(f"Inserindo {record_count} registros na Bronze com batch_id {batch_id}...")
            df_bronze.writeTo("nessie.bronze.vocation").append()
        else:
            logging.warning("Nenhum registro encontrado para gravar na Bronze.")

    #   MÉTODO: SKILLS
    def skills(self):
        """
        Executa o job Bronze para dados de skills.

        Valida, normaliza e insere dados da pasta landing/skills.
        Deduplica por (name, world).

        Tabela criada: nessie.bronze.skills
        """

        self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

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

        path = f"s3a://lakehouse/landing/{partition}/skills/"
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
            .withColumn("vocation", F.trim(F.lower("vocation")))
            .withColumn("world", F.trim(F.lower("world")))
            .dropDuplicates(["name", "world"])
        )

        self.spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

        record_count = df_bronze.count()

        if record_count > 0:
            logging.info(f"Inserindo {record_count} registros na Bronze com batch_id {batch_id}...")
            df_bronze.writeTo("nessie.bronze.skills").append()
        else:
            logging.warning("Nenhum registro encontrado na Bronze Skills.")

    #   MÉTODO: EXTRA
    def extra(self):
        """
        Executa o job Bronze Extra, consolidando e normalizando múltiplos
        arquivos CSV genéricos encontrados na pasta 'extra/'.

        Este job é mais flexível, permitindo múltiplos formatos de arquivo.

        Tabela criada: nessie.bronze.extra
        """

        self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

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

        today_date = datetime.strptime(self.date_str, "%Y-%m-%d") if self.date_str else datetime.today()
        partition = f"year={today_date.year}/month={today_date.month}/day={today_date.day}"
        
        path = f"s3a://lakehouse/landing/{partition}/skills/"
        logging.info(f"Lendo dados de: {path}")

        df_raw = (
            self.spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(path)
        )

        if df_raw.head(1) == []:
            logging.warning("Nenhum arquivo encontrado em extra/. Encerrando Bronze Extra.")
            return

        # Normaliza nomes (lowercase + underscore)
        df_raw = df_raw.toDF(*[c.lower().replace(" ", "_") for c in df_raw.columns])

        # Correção de nomes alternativos
        rename_map = {"score": "points", "skill_level": "points"}
        for old, new in rename_map.items():
            if old in df_raw.columns and new not in df_raw.columns:
                df_raw = df_raw.withColumnRenamed(old, new)

        # Colunas finais esperadas
        final_columns = ["name", "vocation", "level", "world", "category", "title", "points"]

        # Cria colunas faltantes
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
            .withColumn("vocation", F.trim(F.lower("vocation")))
            .withColumn("world", F.trim(F.lower("world")))
            .dropDuplicates(["name", "world", "category"])
        )

        self.spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

        record_count = df_bronze.count()

        if record_count > 0:
            logging.info(f"Inserindo {record_count} registros na Bronze Extra com batch_id {batch_id}...")
            df_bronze.writeTo("nessie.bronze.extra").append()
            logging.info("Carga concluída com sucesso.")
        else:
            logging.warning("Nenhum registro encontrado na Bronze Extra.")
