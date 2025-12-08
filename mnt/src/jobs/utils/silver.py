from datetime import datetime
from uuid import uuid4 
import logging
import sys
from pyspark.files import SparkFiles
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class Silver:
    """
    Classe responsável pelo processamento da camada Silver no Lakehouse.

    Esta camada aplica regras de transformação e versionamento de histórico
    utilizando o padrão **SCD Type 2** com Iceberg e Nessie como catálogo/controle de versões.

    Cada método representa um domínio específico (vocation, skills, extra)
    e realiza:
        - Criação do namespace e tabelas Silver (se não existirem)
        - Leitura dos dados da camada Bronze
        - Aplicação de carimbos de tempo (start/end)
        - Merge/Upsert com histórico (SCD2)
        - Auditoria do resultado

    Parâmetros
    ----------
    spark : SparkSession
        Sessão Spark configurada com Iceberg e Nessie.
    """

    def __init__(self, spark):
        """
        Inicializa a classe Silver.

        Parâmetros
        ----------
        spark : SparkSession
            Instância ativa da SparkSession.
        """
        self.spark = spark

    # =======================================================================
    # VOCATION
    # =======================================================================
    def vocation(self):
        """
        Processa a camada Silver do domínio 'vocation'.

        Fluxo executado:
        ----------------
        1. Configura warehouse e cria namespace no Nessie.
        2. Cria tabela Iceberg particionada por:
            - world
            - days(start_date)
            - bucket(8, name)
        3. Lê dados da Bronze.
        4. Adiciona colunas de controle (start_date, end_date, is_current).
        5. Executa MERGE INTO aplicando SCD Type 2:
            - Atualiza registros alterados finalizando versões antigas.
            - Insere novas versões.
        6. Realiza auditoria final (contagem geral e registros atuais).

        Exceções
        --------
        Levanta exceção e encerra com código 1 em caso de erro.
        """
        try:
            # Configuração e criação de namespace
            self.spark.conf.set("spark.sql.catalog.nessie.warehouse", "s3a://silver/")
            self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver.vocation")

            # Criação da tabela
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

            # Leitura Bronze
            df_bronze = self.spark.read.table("nessie.bronze.vocation")

            if df_bronze.head(1) == []:
                logging.warning("Nenhum dado encontrado na Bronze. Encerrando execução.")
                return

            # Novos registros
            df_new = (
                df_bronze
                .withColumn("start_date", F.current_timestamp())
                .withColumn("end_date", F.lit(None).cast("timestamp"))
                .withColumn("is_current", F.lit(True))
            )

            df_new.createOrReplaceTempView("vocation_updates")
            logging.info("View temporária `vocation_updates` criada com sucesso.")

            # MERGE INTO (SCD2)
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

            # Auditoria
            df_check = self.spark.read.table("nessie.silver.vocation")
            total_rows = df_check.count()
            current_rows = df_check.filter("is_current = true").count()

            logging.info(f"Total de registros na Silver: {total_rows}")
            logging.info(f"Registros atuais (is_current = true): {current_rows}")

        except Exception as e:
            logging.exception(f"Falha no job Silver: {str(e)}")
            sys.exit(1)

    # =======================================================================
    # SKILLS
    # =======================================================================
    def skills(self):
        """
        Processa a camada Silver do domínio 'skills' aplicando SCD Type 2.

        O processo segue a mesma lógica do domínio 'vocation', porém adaptado
        para colunas específicas de skills de personagens.

        Fluxo executado:
        ----------------
        - Criação do namespace e tabela Silver
        - Leitura da Bronze (skills)
        - Criação de colunas de controle temporal
        - MERGE INTO Iceberg com versionamento histórico
        - Auditoria do processamento

        Exceções
        --------
        Em caso de falha, loga a exceção e encerra o processo.
        """
        try:
            self.spark.conf.set("spark.sql.catalog.nessie.warehouse", "s3a://silver/")
            self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver.skills")

            # Criar tabela Silver
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

            # Leitura Bronze
            df_bronze = self.spark.read.table("nessie.bronze.skills")

            if df_bronze.head(1) == []:
                logging.warning("Nenhum dado encontrado na Bronze. Encerrando execução.")
                return

            # Novos registros
            df_new = (
                df_bronze
                .withColumn("start_date", F.current_timestamp())
                .withColumn("end_date", F.lit(None).cast("timestamp"))
                .withColumn("is_current", F.lit(True))
            )

            df_new.createOrReplaceTempView("skills_updates")
            logging.info("View temporária 'skills_updates' criada com sucesso.")

            # MERGE (SCD2)
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

            # Auditoria
            df_check = self.spark.read.table("nessie.silver.skills")
            total_rows = df_check.count()
            current_rows = df_check.filter("is_current = true").count()

            logging.info(f"Total de registros na Silver: {total_rows}")
            logging.info(f"Registros atuais (is_current = true): {current_rows}")

        except Exception as e:
            logging.exception(f"Falha no job Silver (skills): {str(e)}")
            sys.exit(1)
