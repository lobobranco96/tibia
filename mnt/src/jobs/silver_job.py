"""
Módulo de Execução da Camada Silver
-----------------------------------

Este script executa o processamento da camada Silver no Lakehouse Tibia Highscores.
Ele recebe como argumento o tipo de operação desejada (vocation | skills | extra)
e executa dinamicamente o método correspondente disponível na classe `Silver`.

Fluxo geral:
1. Lê parâmetros enviados pela linha de comando.
2. Valida o tipo de operação recebido.
3. Cria uma SparkSession configurada via `create_spark_session`.
4. Instancia a classe Silver, responsável por aplicar as transformações da camada Silver.
5. Executa dinamicamente o método correspondente ao tipo solicitado.
6. Registra logs no padrão operacional da pipeline.
7. Finaliza com erro caso a operação não exista.

Este módulo é utilizado pelo SparkSubmitOperator dentro do Airflow.
"""

from utils.utility import create_spark_session
from utils.bronze import Silver
from pyspark.sql import functions as F
from datetime import datetime
from uuid import uuid4
import logging
import sys

# -----------------------------------------------------------
# Configuração de Logging
# -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Tipos válidos de processamento na camada Silver
VALID_TYPES = {"vocation", "skills", "extra"}


def parse_date_argument() -> str | None:
    """
    Lê o argumento opcional '--date' fornecido via CLI.

    Returns
    -------
    str | None
        A data no formato YYYY-MM-DD caso fornecida.
        Retorna None caso o parâmetro não esteja presente.

    Exemplo:
        python silver_job.py vocation --date 2025-01-15
    """
    if "--date" in sys.argv:
        return sys.argv[sys.argv.index("--date") + 1]
    return None


def validate_operation_type(operation: str) -> None:
    """
    Valida se o tipo de operação enviado é permitido.

    Parameters
    ----------
    operation : str
        Nome da operação desejada (vocation | skills | extra).

    Raises
    ------
    SystemExit
        Caso a operação não seja informada ou seja inválida.
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
    Entrada principal do script Silver.

    Este bloco:
    - Lê e valida argumentos
    - Cria a SparkSession configurada para Silver
    - Instancia a classe Silver
    - Executa dinamicamente o método correspondente ao tipo solicitado
    - Gera logs estruturados para auditoria da pipeline
    """

    # Lê parâmetro opcional de data
    date_str = parse_date_argument()

    # Verifica se o tipo de operação foi passado
    if len(sys.argv) < 2:
        validate_operation_type(None)

    # Lê operação solicitada
    silver_type = sys.argv[1].lower()
    validate_operation_type(silver_type)

    try:
        # Cria SparkSession nomeada conforme tipo
        spark = create_spark_session(f"silver_{silver_type}")

        # Instancia a camada Silver
        silver = Silver(spark)

        # Executa método de forma dinâmica
        if hasattr(silver, silver_type):
            method = getattr(silver, silver_type)
            method()
            logging.info("Camada silver finalizada com sucesso.")
        else:
            raise AttributeError(
                f"O método '{silver_type}' não existe na classe Silver."
            )

    except Exception as e:
        logging.exception(f"Falha no job Silver: {str(e)}")
        sys.exit(1)
