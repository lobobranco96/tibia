from utils.utility import create_spark_session
from utils.gold import Gold
import logging


if __name__ == "__main__":
    """
    Entrada principal do script Gold.

    Este bloco:
    - Cria a SparkSession configurada para Gold
    - Instancia a classe Gold
    - Executa as transformações da camada Gold
    - Gera logs estruturados para auditoria da pipeline
    """

    spark = None

    try:
        spark = create_spark_session("Gold")

        gold = Gold(spark)

        gold.experience_global_rank()
        gold.skills_global_rank()
        gold.world_summary()
        gold.player_progression()
        gold.skills_progression()

        logging.info("Job Gold executado com sucesso.")

    except Exception as e:
        logging.exception(f"Falha no job Gold: {str(e)}")

    finally:
        if spark:
            spark.stop()
            logging.info("Sessão Spark Gold encerrada com sucesso.")
