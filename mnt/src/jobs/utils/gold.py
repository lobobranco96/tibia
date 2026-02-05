import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class Gold:
    def __init__(self, spark):
        self.spark = spark

    def experience_rank_atualizado(self):

        logging.info("Atualizando ranking global de experiencia.")

        return self.spark.sql("""
            INSERT INTO nessie.gold.experience_rank_global
            SELECT
                ROW_NUMBER() OVER (
                    PARTITION BY DATE(current_timestamp())
                    ORDER BY level DESC, experience DESC, name ASC
                ) AS rank,
                name,
                world,
                vocation,
                level,
                experience,
                world_type,
                current_timestamp() AS updated_at,
                DATE(current_timestamp()) AS snapshot_date
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
    
    def experience_progression(self):

        logging.info("Atualizando evolução de level e experiência por player.")

        return self.spark.sql("""
            CREATE OR REPLACE TABLE nessie.gold.experience_progression
            USING iceberg
            AS
            WITH ordered_vocation AS (
                SELECT
                    name,
                    world,
                    vocation,
                    world_type,

                    CAST(level AS INT) AS level,
                    CAST(experience AS BIGINT) AS experience,
                    CAST(start_date AS TIMESTAMP) AS start_date,
                    CAST(is_current AS BOOLEAN) AS is_current,

                    LAG(CAST(level AS INT)) OVER (
                        PARTITION BY name
                        ORDER BY start_date
                    ) AS previous_level,

                    LAG(CAST(experience AS BIGINT)) OVER (
                        PARTITION BY name
                        ORDER BY start_date
                    ) AS previous_experience,

                    LAG(start_date) OVER (
                        PARTITION BY name
                        ORDER BY start_date
                    ) AS previous_start_date

                FROM nessie.silver.vocation
            )

            SELECT
                name,
                world,
                vocation,
                world_type,

                previous_level,
                level AS current_level,
                (level - previous_level) AS level_gain,

                previous_experience,
                experience AS current_experience,
                (experience - previous_experience) AS experience_gain,

                previous_start_date,
                start_date AS current_start_date,

                DATEDIFF(
                    CAST(start_date AS DATE),
                    CAST(previous_start_date AS DATE)
                ) AS days_between_updates,

                ROUND(
                    (experience - previous_experience) /
                    GREATEST(
                        DATEDIFF(
                            CAST(start_date AS DATE),
                            CAST(previous_start_date AS DATE)
                        ),
                        1
                    ),
                    2
                ) AS avg_xp_per_day,

                is_current,
                current_timestamp() AS updated_at

            FROM ordered_vocation
            WHERE previous_level IS NOT NULL
        """)
    
    def skills_progression(self):

        logging.info("Atualizando progressão de skills por jogador.")

        return self.spark.sql("""
            CREATE OR REPLACE TABLE nessie.gold.skills_progression
            USING iceberg
            AS
            WITH ordered_progression AS (
                SELECT
                    name,
                    world,
                    vocation,
                    category,

                    CAST(skill_level AS INT) AS skill_level,
                    CAST(start_date AS TIMESTAMP) AS start_date,
                    CAST(is_current AS BOOLEAN) AS is_current,

                    LAG(CAST(skill_level AS INT)) OVER (
                        PARTITION BY name, category
                        ORDER BY start_date
                    ) AS previous_skill_level,

                    LAG(start_date) OVER (
                        PARTITION BY name, category
                        ORDER BY start_date
                    ) AS previous_start_date

                FROM nessie.silver.skills
            )

            SELECT
                name,
                world,
                vocation,
                category,

                previous_skill_level AS skill_before,
                skill_level AS skill_after,
                (skill_level - previous_skill_level) AS skill_gain,

                previous_start_date AS from_date,
                start_date AS to_date,

                DATEDIFF(
                    CAST(start_date AS DATE),
                    CAST(previous_start_date AS DATE)
                ) AS days_between_updates,

                ROUND(
                    (skill_level - previous_skill_level) /
                    NULLIF(
                        DATEDIFF(
                            CAST(start_date AS DATE),
                            CAST(previous_start_date AS DATE)
                        ),
                        0
                    ),
                    2
                ) AS avg_skill_per_day,

                is_current,
                current_timestamp() AS updated_at

            FROM ordered_progression
            WHERE previous_skill_level IS NOT NULL
        """)