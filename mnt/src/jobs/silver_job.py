from utils.utility import create_spark_session
from utils.bronze import Silver
from pyspark.sql import functions as F
from datetime import datetime
from uuid import uuid4 
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

VALID_TYPES = {"vocation", "skills", "extra"}


if __name__ == "__main__":
    date_str = None
    if "--date" in sys.argv:
        date_str = sys.argv[sys.argv.index("--date") + 1]

    if len(sys.argv) < 2:
        logging.error("Erro: Nenhum tipo de operação informado (vocation | skills | extra).")
        sys.exit(1)

    silver_type = sys.argv[1].lower()

    if silver_type not in VALID_TYPES:
        logging.error(
            f"Erro: Tipo de operação inválido: '{silver_type}'. "
            f"Use um dos valores: {VALID_TYPES}"
        )
        sys.exit(1)

    try:
        spark = create_spark_session(f"silver_{silver_type}")
        silver = Silver(spark)
        # Chama dinamicamente o método certo
        if hasattr(silver, silver_type):
            method = getattr(silver, silver_type)
            method()
            logging.info("Camada silver finalizada.")
        else:
            raise AttributeError(f"'{silver_type}' não possui método correspondente no Silver")


    except Exception as e:
        logging.exception(f"Falha no job Silver: {str(e)}")
        sys.exit(1)