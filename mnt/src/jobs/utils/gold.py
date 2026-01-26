import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class Gold:
    def __init__(self, spark):
        self.spark = spark

    def experience_rank_atualizado(self):

        logging.info("Atualizando ranking global de experiencia.")

        return self.spark.sql("""
                CREATE OR REPLACE TABLE nessie.gold.experience_rank_global
                USING iceberg
                AS
                SELECT
                    ROW_NUMBER() OVER (
                        ORDER BY level DESC, experience DESC, name ASC
                        ) AS rank
                    , name
                    , world
                    , vocation
                    , level
                    , experience
                    , world_type
                    , current_timestamp() AS updated_at
                FROM nessie.silver.vocation
                WHERE is_current = true;
                                """)


    def skills_rank_atualizado(self):

        logging.info("Atualizando ranking global de skills.")

        return self.spark.sql("""
            CREATE OR REPLACE TABLE nessie.gold.skills_rank_global
            USING iceberg
            AS
            SELECT
                  name
                , world
                , category AS skill_name
                , vocation
                , skill_level
                , current_timestamp() AS updated_at
        FROM nessie.silver.skills
        WHERE is_current = true
        """)

    def world_summary(self):

        logging.info("Atualizando resumo de players por world e vocation.")

        return self.spark.sql("""
            CREATE OR REPLACE TABLE nessie.gold.world_summary
            USING iceberg
            AS
            SELECT
                world
                , world_type
                , vocation
                , COUNT(DISTINCT name) AS players_count
                , current_timestamp() AS updated_at
            FROM nessie.silver.vocation
            WHERE is_current = true
            GROUP BY
                world
                , world_type
                , vocation
        """)