from utils.utility import create_spark_session
from utils.bronze import Bronze
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

    bronze_type = sys.argv[1].lower()

    if bronze_type not in VALID_TYPES:
        logging.error(
            f"Erro: Tipo de operação inválido: '{bronze_type}'. "
            f"Use um dos valores: {VALID_TYPES}"
        )
        sys.exit(1)

    try:
        spark = create_spark_session(f"bronze_{bronze_type}")
        bronze = Bronze(spark, date_str)
        # Chama dinamicamente o método certo
        if hasattr(bronze, bronze_type):
            method = getattr(bronze, bronze_type)
            method()
            logging.info(f"Camada bronze finalizada.")
        else:
            raise AttributeError(f"'{bronze_type}' não possui método correspondente no Bronze")


    except Exception as e:
        logging.exception(f"Falha no job Bronze: {str(e)}")
        sys.exit(1)