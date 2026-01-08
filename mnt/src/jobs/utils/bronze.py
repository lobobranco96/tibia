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

    def __init__(self, spark, date_str=None):
        self.spark = spark
        self.date_str = date_str
        
        # Compressão padrão Parquet
        self.spark.conf.set(
            "spark.sql.parquet.compression.codec", "snappy"
        )

    
    #   MÉTODO: VOCATION
    def vocation(self):
        """
        Executa o job Bronze para dados de vocação (vocation).

        1. Configura catálogo Iceberg + warehouse.
        3. Lê arquivos CSV da camada landing.
        4. Valida colunas obrigatórias.
        5. Normaliza nomes e tipos de colunas.
        6. Gera batch_id e colunas de auditoria.
        7. Deduplica por (name, world).
        8. Insere incrementalmente na tabela Bronze.

        Tabela criada: nessie.bronze.vocation
        """

        # Define path da landing com base na data
        today_date = datetime.strptime(self.date_str, "%Y-%m-%d") if self.date_str else datetime.today()
        partition = (
                    f"year={today_date.year}/"
                    f"month={today_date.strftime('%m')}/"
                    f"day={today_date.strftime('%d')}"
                    )
        

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

        
        # Normalização e padronização
        df_bronze = (
            df_raw.drop("Rank")
            .selectExpr(
                "Name as name",
                "lower(trim(Vocation)) as vocation",
                "lower(trim(World)) as world",
                "cast(Level as int) as level",
                "cast(regexp_replace(Points, ',', '') as long) as experience",
                "WorldType as world_type")
            .withColumn("ingestion_time", F.current_timestamp())
            .withColumn("ingestion_date", F.current_date())
            .withColumn("source_system", F.lit("highscore_tibia_page"))
            .withColumn("batch_id", F.lit(batch_id))
            .dropDuplicates(["name", "world"])
        )
        
        logging.info(f"Inserindo registros na Bronze com batch_id {batch_id}...")
        df_bronze.writeTo("nessie.bronze.vocation").append()
        logging.info("Carga concluída com sucesso.")

    
    #   MÉTODO: SKILLS
    def skills(self):
        """
        Executa o job Bronze para dados de skills.

        Valida, normaliza e insere dados da pasta landing/skills.
        Deduplica por (name, world).

        Tabela criada: nessie.bronze.skills
        """

        today_date = datetime.strptime(self.date_str, "%Y-%m-%d") if self.date_str else datetime.today()
        partition = (
                    f"year={today_date.year}/"
                    f"month={today_date.strftime('%m')}/"
                    f"day={today_date.strftime('%d')}"
                    )
        
        

        path = f"s3a://lakehouse/landing/{partition}/skills/"
        logging.info(f"Lendo dados de: {path}")

        df_raw = self.spark.read.csv(path, header=True)

        colunas_esperadas = {'Rank', 'Name', 'Vocation', 'World', 'Skill Level', 'Category'}
        colunas_faltando = colunas_esperadas - set(df_raw.columns)

        if colunas_faltando:
            logging.error(f"Colunas ausentes no CSV: {colunas_faltando}")
            return

        batch_id = str(uuid4())
        logging.info(f"Gerando batch_id: {batch_id}")
        
        df_bronze = (
            df_raw
            .selectExpr(
                "Name as name",
                "lower(trim(Vocation)) as vocation",
                "lower(trim(World)) as world",
                "cast(`Skill Level` as int) as skill_level",
                "Category as category"
            )
            .withColumn("ingestion_time", F.current_timestamp())
            .withColumn("ingestion_date", F.current_date())
            .withColumn("source_system", F.lit("highscore_tibia_page"))
            .withColumn("batch_id", F.lit(batch_id))
            .dropDuplicates(["name", "world"])
        )

        logging.info(f"Inserindo registros na Bronze com batch_id {batch_id}...")
        df_bronze.writeTo("nessie.bronze.skills").append()
        logging.info("Carga concluída com sucesso.")
        
    #   MÉTODO: EXTRA
    def extra(self):
        """
        Executa o job Bronze Extra, consolidando e normalizando múltiplos
        arquivos CSV genéricos encontrados na pasta 'extra/'.

        Este job é mais flexível, permitindo múltiplos formatos de arquivo.

        Tabela criada: nessie.bronze.extra
        """
        today_date = datetime.strptime(self.date_str, "%Y-%m-%d") if self.date_str else datetime.today()
        partition = (
                    f"year={today_date.year}/"
                    f"month={today_date.strftime('%m')}/"
                    f"day={today_date.strftime('%d')}"
                    )
        
        path = f"s3a://lakehouse/landing/{partition}/extra/"
        logging.info(f"Lendo dados de: {path}")

        df_raw = self.spark.read.csv(path, header=True)

        # Normaliza nomes (lowercase + underscore)
        df_raw = df_raw.toDF(*[c.lower().replace(" ", "_") for c in df_raw.columns])

        # Normaliza nomes alternativos
        rename_map = {"score": "points"}
        df_raw = df_raw.selectExpr(
            *[
                f"{c} as {rename_map.get(c, c)}"
                for c in df_raw.columns
            ]
        )
        
        final_columns = ["name", "vocation", "world", "category", "title", "points"]
        
        # Garante schema final
        df_bronze = df_raw.select(
            *[
                F.col(c) if c in df_raw.columns else F.lit(None).alias(c)
                for c in final_columns
            ]
        )
        
        batch_id = str(uuid4())
        
        df_bronze = (
            df_bronze
            .selectExpr(
                "*",
                "cast(points as int) as points",
                "lower(trim(vocation)) as vocation",
                "lower(trim(world)) as world"
            )
            .withColumn("ingestion_time", F.current_timestamp())
            .withColumn("ingestion_date", F.current_date())
            .withColumn("source_system", F.lit("highscore_tibia_page"))
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("source_file", F.input_file_name())
        )
        
        logging.info(f"Inserindo registros na Bronze Extra com batch_id {batch_id}...")
        df_bronze.writeTo("nessie.bronze.extra").append()
        logging.info("Carga concluída com sucesso.")

