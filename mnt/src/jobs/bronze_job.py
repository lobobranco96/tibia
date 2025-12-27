"""
Módulo de Execução da Camada Bronze
-----------------------------------

Este script é responsável por iniciar o processamento da camada Bronze
do Lakehouse Tibia Highscores. Ele recebe como argumento um tipo de operação
(vocation | skills | extra) e executa dinamicamente o método correspondente
da classe `Bronze`.

Fluxo geral:
1. Valida o tipo de operação solicitado.
2. Cria a SparkSession configurada via `create_spark_session`.
3. Instancia a classe Bronze, que contém os métodos de ingestão.
4. Executa dinamicamente o método conforme o argumento (ex: bronze.vocation()).
5. Registra logs estruturados para auditoria.
6. Permite execução com parâmetro opcional de data (--date YYYY-MM-DD).

Este módulo é usado pelos jobs SparkSubmitOperator no Airflow.
"""

from utils.utility import create_spark_session
from utils.bronze import Bronze
from pyspark.sql import functions as F
from datetime import datetime
from uuid import uuid4
import logging
import sys
from typing import Optional

# -----------------------------------------------------------
# Configuração de logging
# -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Tipos válidos de execução
VALID_TYPES = {"vocation", "skills", "extra"}


def parse_date_argument() -> Optional[str]:
    """
    Lê o argumento opcional '--date' da linha de comando.

    Returns
    -------
    str | None
        String da data no formato YYYY-MM-DD caso fornecida,
        ou None caso não esteja presente.

    Exemplo:
    --------
    python bronze_job.py vocation --date 2025-01-10
    """
    if "--date" in sys.argv:
        return sys.argv[sys.argv.index("--date") + 1]
    return None


def validate_operation_type(operation: str) -> None:
    """
    Valida o tipo de operação solicitado no argumento principal.

    Parameters
    ----------
    operation : str
        Tipo da camada bronze a ser executada (vocation | skills | extra)

    Raises
    ------
    SystemExit
        Se o tipo for inválido ou não for informado.
    """
    if not operation:
        logging.error("Erro: Nenhum tipo de operação informado (vocation | skills | extra).")
        sys.exit(1)

    if operation not in VALID_TYPES:
        logging.error(
            f"Erro: Tipo de operação inválido: '{operation}'. "
            f"Use um dos valores: {VALID_TYPES}"
        )
        sys.exit(1)


# -----------------------------------------------------------
# Execução principal
# -----------------------------------------------------------
if __name__ == "__main__":
    """
    Entrada principal do job Bronze.

    Este bloco:
    - Valida argumentos
    - Cria a SparkSession
    - Inicializa a classe Bronze
    - Executa dinamicamente o método requisitado
    """

    # Lê data opcional
    date_str = parse_date_argument()

    # Lê operação
    if len(sys.argv) < 2:
        validate_operation_type(None)

    bronze_type = sys.argv[1].lower()
    validate_operation_type(bronze_type)

    try:
        # Cria sessão Spark nomeada
        spark = create_spark_session(f"bronze_{bronze_type}")

        # Instancia classe Bronze com Spark e data opcional
        bronze = Bronze(spark, date_str)

        # Executa dinamicamente o método correspondente
        if hasattr(bronze, bronze_type):
            method = getattr(bronze, bronze_type)
            method()
            logging.info("Camada bronze finalizada com sucesso.")
        else:
            raise AttributeError(
                f"O método '{bronze_type}' não existe na classe Bronze."
            )

    except Exception as e:
        logging.exception(f"Falha no job Bronze: {str(e)}")
        sys.exit(1)
