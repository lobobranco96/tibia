import logging
import sys
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
            self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

            # Criação da tabela
            self.spark.sql("""
            CREATE TABLE IF NOT EXISTS nessie.silver.vocation (
                name STRING,
                world STRING,
                vocation STRING,
                level INT,
                experience LONG,
                world_type STRING,
                ingestion_time TIMESTAMP,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                is_current BOOLEAN,
                hash_diff STRING
            )
            USING iceberg
            PARTITIONED BY (world)
            TBLPROPERTIES (
                'format-version' = '2', 
                'write.update.mode' = 'copy-on-write',
                'write.delete.mode' = 'copy-on-write'
            )
            """)
            self.spark.conf.set(
                "spark.sql.iceberg.write.distribution-mode",
                "none"
            )
            self.spark.conf.set("spark.sql.files.maxRecordsPerFile", 500000)
            self.spark.conf.set("spark.sql.iceberg.write.target-file-size-bytes", 134217728)  # 128MB
            logging.info("Tabela Silver inicializada com sucesso.")

            # Leitura Bronze
            df_bronze_all = self.spark.read.table("nessie.bronze.vocation")
            last_batch_id = (
                df_bronze_all
                .orderBy(F.col("ingestion_time").desc())
                .select("batch_id")
                .first()["batch_id"]
            )

            logging.info(f"Processando batch_id: {last_batch_id}")

            df_bronze = df_bronze_all.filter(F.col("batch_id") == last_batch_id)
            if df_bronze.head(1) == []:
                logging.warning("Nenhum dado encontrado na Bronze. Encerrando execução.")
                return

            # Novos registros
            df_new = (
                df_bronze
                .withColumn(
                    "hash_diff",
                    F.sha2(
                        F.concat_ws(
                            "||",
                            F.col("vocation"),
                            F.col("level").cast("string"),
                            F.col("experience").cast("string"),
                            F.col("world_type")
                        ),
                        256
                    )
                )
                .withColumn("start_date", F.current_timestamp())
                .withColumn("end_date", F.lit(None).cast("timestamp"))
                .withColumn("is_current", F.lit(True))
            )

            df_new = df_new.repartition(4, "world") 
            df_new.createOrReplaceTempView("vocation_updates")
            logging.info("View temporária `vocation_updates` criada com sucesso.")

            # MERGE INTO (SCD2)
            merge_update_query = """
            MERGE INTO nessie.silver.vocation t
            USING vocation_updates s
            ON  t.name = s.name
            AND t.world = s.world
            AND t.is_current = TRUE

            WHEN MATCHED
            AND t.hash_diff <> s.hash_diff
            THEN UPDATE SET
                t.end_date = current_timestamp(),
                t.is_current = FALSE
            """
            self.spark.sql(merge_update_query)
            logging.info("MERGE INTO finalizado com sucesso!")

            insert_query = """
            INSERT INTO nessie.silver.vocation
            SELECT
                s.name,
                s.world,
                s.vocation,
                s.level,
                s.experience,
                s.world_type,
                s.ingestion_time,
                current_timestamp() AS start_date,
                NULL AS end_date,
                TRUE AS is_current,
                s.hash_diff
            FROM vocation_updates s
            LEFT ANTI JOIN nessie.silver.vocation t
            ON t.name = s.name
            AND t.world = s.world
            AND t.is_current = TRUE
            """
            self.spark.sql(insert_query)



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
            self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

            # Criar tabela Silver
            self.spark.sql("""
            CREATE TABLE IF NOT EXISTS nessie.silver.skills (
                name STRING,
                world STRING,
                category STRING,
                vocation STRING,
                skill_level INT,
                ingestion_time TIMESTAMP,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                is_current BOOLEAN,
                hash_diff STRING
            )
            USING iceberg
            PARTITIONED BY (world)
            TBLPROPERTIES (
                'format-version' = '2', 
                'write.update.mode' = 'copy-on-write',
                'write.delete.mode' = 'copy-on-write'
            )
            """)

            self.spark.conf.set(
                "spark.sql.iceberg.write.distribution-mode",
                "none"
            )
            self.spark.conf.set("spark.sql.files.maxRecordsPerFile", 500000)
            self.spark.conf.set("spark.sql.iceberg.write.target-file-size-bytes", 134217728)  # 128MB

            logging.info("Tabela Silver 'skills' inicializada com sucesso.")

            # Leitura Bronze
            df_bronze_all = self.spark.read.table("nessie.bronze.skills")
            last_batch_id = (
                df_bronze_all
                .orderBy(F.col("ingestion_time").desc())
                .select("batch_id")
                .first()["batch_id"]
            )
            
            logging.info(f"Processando batch_id: {last_batch_id}")
            df_bronze = df_bronze_all.filter(F.col("batch_id") == last_batch_id)
            if df_bronze.head(1) == []:
                logging.warning("Nenhum dado encontrado na Bronze. Encerrando execução.")
                return

            # Novos registros
            df_new = (
                df_bronze
                .withColumn(
                    "hash_diff",
                    F.sha2(
                        F.concat_ws(
                            "||",
                            F.col("vocation"),
                            F.col("skill_level").cast("string")
                        ),
                        256
                    )
                )
                .withColumn("start_date", F.current_timestamp())
                .withColumn("end_date", F.lit(None).cast("timestamp"))
                .withColumn("is_current", F.lit(True))
            )

            df_new.createOrReplaceTempView("skills_updates")
            logging.info("View temporária 'skills_updates' criada com sucesso.")

            merge_update_query = """
            MERGE INTO nessie.silver.skills t
            USING skills_updates s
            ON  t.name = s.name
            AND t.world = s.world
            AND t.category = s.category
            AND t.is_current = TRUE

            WHEN MATCHED
            AND t.hash_diff <> s.hash_diff
            THEN UPDATE SET
                t.end_date = current_timestamp(),
                t.is_current = FALSE
            """
            self.spark.sql(merge_update_query)
            logging.info("UPDATE finalizado com sucesso!")

            insert_query = """
            INSERT INTO nessie.silver.skills
            SELECT
                s.name,
                s.world,
                s.category,
                s.vocation,
                s.level,
                s.skill_level,
                s.ingestion_time,
                current_timestamp() AS start_date,
                NULL AS end_date,
                TRUE AS is_current,
                s.hash_diff
            FROM skills_updates s
            LEFT ANTI JOIN nessie.silver.skills t
            ON  t.name = s.name
            AND t.world = s.world
            AND t.category = s.category
            AND t.is_current = TRUE
            """
            self.spark.sql(insert_query)
            logging.info("INSERT finalizado com sucesso!")


            # Auditoria
            df_check = self.spark.read.table("nessie.silver.skills")
            total_rows = df_check.count()
            current_rows = df_check.filter("is_current = true").count()

            logging.info(f"Total de registros na Silver: {total_rows}")
            logging.info(f"Registros atuais (is_current = true): {current_rows}")

        except Exception as e:
            logging.exception(f"Falha no job Silver (skills): {str(e)}")
            sys.exit(1)

    # =======================================================================
    # EXTRA
    # =======================================================================
    def extra(self):
        """
        Processa a camada Silver do domínio 'extra' aplicando SCD Type 2.
    
        Chave de negócio:
            (name, world, category, title)
    
        Campos versionados:
            - points
            - level
            - vocation
        """
        try:
            self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    
            # Criação da tabela Silver Extra
            self.spark.sql("""
            CREATE TABLE IF NOT EXISTS nessie.silver.extra (
                name STRING,
                world STRING,
                category STRING,
                title STRING,
                vocation STRING,
                points INT,
                source_file STRING,
                ingestion_time TIMESTAMP,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                is_current BOOLEAN,
                hash_diff STRING
            )
            USING iceberg
            PARTITIONED BY (world)
            TBLPROPERTIES (
                'format-version' = '2', 
                'write.update.mode' = 'copy-on-write',
                'write.delete.mode' = 'copy-on-write'
            )
            """)
            self.spark.conf.set(
                "spark.sql.iceberg.write.distribution-mode",
                "none"
            )
            self.spark.conf.set("spark.sql.files.maxRecordsPerFile", 500000)
            self.spark.conf.set("spark.sql.iceberg.write.target-file-size-bytes", 134217728)  # 128MB
    
            logging.info("Tabela Silver 'extra' inicializada com sucesso.")
    
            # Leitura da Bronze
            df_bronze_all = self.spark.read.table("nessie.bronze.extra")
            last_batch_id = (
                df_bronze_all
                .select("batch_id", "ingestion_time")
                .orderBy(F.col("ingestion_time").desc())
                .limit(1)
                .collect()[0]["batch_id"]
            )
    
            logging.info(f"Processando batch_id: {last_batch_id}")
    
            df_bronze = df_bronze_all.filter(F.col("batch_id") == last_batch_id)
    
            if df_bronze.head(1) == []:
                logging.warning("Nenhum dado encontrado na Bronze Extra.")
                return
    
            # Preparação da source (SEM start_date / is_current)
            df_updates = (
                df_bronze
                .withColumn(
                    "hash_diff",
                    F.sha2(
                        F.concat_ws(
                            "||",
                            F.col("vocation"),
                            F.col("level").cast("string"),
                            F.col("points").cast("string")
                        ),
                        256
                    )
                )
            )
    
            df_updates.createOrReplaceTempView("extra_updates")
            logging.info("View temporária 'extra_updates' criada com sucesso.")

            merge_update_query = """
            MERGE INTO nessie.silver.extra t
            USING extra_updates s
            ON  t.name = s.name
            AND t.world = s.world
            AND t.category = s.category
            AND t.title <=> s.title
            AND t.is_current = TRUE

            WHEN MATCHED
            AND t.hash_diff <> s.hash_diff
            THEN UPDATE SET
                t.end_date = current_timestamp(),
                t.is_current = FALSE
            """
            self.spark.sql(merge_update_query)
            logging.info("UPDATE finalizado com sucesso!")

            insert_query = """
            INSERT INTO nessie.silver.extra
            SELECT
                s.name,
                s.world,
                s.category,
                s.title,
                s.vocation,
                s.level,
                s.points,
                s.source_file,
                s.ingestion_time,
                current_timestamp() AS start_date,
                NULL AS end_date,
                TRUE AS is_current,
                s.hash_diff
            FROM extra_updates s
            LEFT ANTI JOIN nessie.silver.extra t
            ON  t.name = s.name
            AND t.world = s.world
            AND t.category = s.category
            AND t.title <=> s.title
            AND t.is_current = TRUE
            """ 
            self.spark.sql(insert_query)
            logging.info("INSERT finalizado com sucesso!")
    
            # Auditoria
            df_check = self.spark.read.table("nessie.silver.extra")
    
            total_rows = df_check.count()
            current_rows = df_check.filter("is_current = true").count()
    
            logging.info(f"Total de registros: {total_rows}")
            logging.info(f"Registros atuais (is_current = true): {current_rows}")
    
        except Exception as e:
            logging.exception(f"Falha no job Silver Extra: {str(e)}")
            sys.exit(1)

