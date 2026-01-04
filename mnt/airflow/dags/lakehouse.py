from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


"""
Pipeline Lakehouse – Tibia Highscores
-------------------------------------

Este módulo define uma DAG Airflow responsável por orquestrar o processamento
dos dados nas camadas Bronze e Silver do Lakehouse baseado em Apache Iceberg
e Spark. A pipeline depende da execução prévia da DAG de Landing
(`tibia_highscores_pipeline`) e utiliza tarefas SparkSubmitOperator para processar
os dados de vocações, skills e informações extras.

Fluxo Geral:
    1. Aguardamos a DAG de Landing (coleta bruta) finalizar.
    2. Executamos o processamento Bronze (padronização e limpeza inicial).
    3. Executamos o processamento Silver (modelagem e estrutura refinada).
    4. Finalizamos a pipeline.

Tecnologias principais:
    - Apache Spark com Iceberg e Nessie
    - Airflow (TaskGroup, ExternalTaskSensor, SparkSubmitOperator)
    - Orquestração em camadas da arquitetura Medallion
"""

BRONZE_SCRIPT = "/opt/airflow/dags/src/jobs/bronze_job.py"
SILVER_SCRIPT = "/opt/airflow/dags/src/jobs/silver_job.py"

default_args = {
    "owner": "lobobranco",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def spark_task(task_id, app_path, args=None):
    """
    Cria uma tarefa SparkSubmitOperator configurada para executar jobs Spark no cluster.

    Parameters
    ----------
    task_id : str
        Identificador único da task no Airflow.
    app_path : str
        Caminho do script PySpark a ser executado.
    args : list | None
        Lista de argumentos a serem enviados ao script PySpark. Caso seja None,
        nenhuma aplicação de argumentos será enviada.

    Returns
    -------
    SparkSubmitOperator
        Uma tarefa Airflow configurada para submissão de jobs Spark.

    Observações
    -----------
    Esta função encapsula as configurações necessárias para uso de:
    - Iceberg + Nessie
    - Credenciais AWS para S3/MinIO
    - Inclusão de .jars obrigatórias para transações ACID e catálogo
    """
    conf = {
        "spark.jars": ",".join([
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
            "/opt/spark/jars/hadoop-aws-3.3.4.jar",
            "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
            "/opt/spark/jars/nessie-spark-extensions-3.5_2.12-0.99.0.jar",
            "/opt/spark/jars/iceberg-aws-bundle-1.6.1.jar",
            "/opt/spark/jars/bundle-2.28.13.jar"
        ])
    }

    return SparkSubmitOperator(
        task_id=task_id,
        application=app_path,
       # pool="spark_pool",
        conn_id="spark_default",
        conf=conf,
        py_files="/opt/airflow/dags/src/jobs",
        verbose=True,
        application_args=args if args else None
    )


@dag(
    dag_id="lakehouse_pipeline",
    description="Lakehouse pipeline: processa dados da camada Bronze até Silver utilizando Spark + Iceberg.",
    default_args=default_args,
    start_date=datetime(2025, 12, 27),
    tags=["tibia", "lakehouse", "etl"]
)
def lakehouse_pipeline():
    """
    DAG responsável por orquestrar o processamento das camadas Bronze e Silver do Lakehouse Tibia.

    Fluxo:
        - Aguarda a finalização da DAG de Landing.
        - Inicia a pipeline com BashOperator.
        - Executa tasks Bronze e Silver em paralelo por categoria.
        - Finaliza a pipeline.

    O TaskGroup "lakehouse" agrupa todos os processamentos Spark.
    """

    # Espera a DAG de landing concluir

    wait_vocation = S3KeySensor(
         task_id="wait_for_vocation_landing",
         bucket_name="lakehouse",
         bucket_key=(
             "landing/"
             "year={{ data_interval_start.strftime('%Y') }}/"
             "month={{ data_interval_start.strftime('%m') }}/"
             "day={{ data_interval_start.strftime('%d') }}/"
             "experience/_SUCCESS"
         ),
         aws_conn_id="minio_s3",
         deferrable=True,
         poke_interval=60,
         timeout=60 * 60 * 3,
     )

    wait_skills = S3KeySensor(
         task_id="wait_for_skills_landing",
         bucket_name="lakehouse",
         bucket_key=(
             "landing/"
             "year={{ data_interval_start.strftime('%Y') }}/"
             "month={{ data_interval_start.strftime('%m') }}/"
             "day={{ data_interval_start.strftime('%d') }}/"
             "skills/_SUCCESS"
         ),
         aws_conn_id="minio_s3",
         deferrable=True,
         poke_interval=60,
         timeout=60 * 60 * 3,
     )

    wait_extra = S3KeySensor(
        task_id="wait_for_extra_landing",
        bucket_name="lakehouse",
        bucket_key=(
            "landing/"
            "year={{ data_interval_start.strftime('%Y') }}/"
            "month={{ data_interval_start.strftime('%m') }}/"
            "day={{ data_interval_start.strftime('%d') }}/"
            "extra/_SUCCESS"
        ),
        aws_conn_id="minio_s3",
        deferrable=False,
        poke_interval=60,
        timeout=60 * 60 * 3,
    )
    with TaskGroup(group_id="vocation_lakehouse") as vocation_group:

        bronze_vocation = spark_task(
             "bronze_vocation",
             BRONZE_SCRIPT,
             args=["vocation"]#, "--date", "2026-01-03"]
         )

        silver_vocation = spark_task(
             "silver_vocation",
             SILVER_SCRIPT,
             args=["vocation"]
         )

        bronze_vocation >> silver_vocation

    with TaskGroup(group_id="skills_lakehouse") as skills_group:

        bronze_skills = spark_task(
             "bronze_skills",
             BRONZE_SCRIPT,
             args=["skills"]#,~ "--date", "2026-01-03"]
         )

        silver_skills = spark_task(
             "silver_skills",
             SILVER_SCRIPT,
             args=["skills"]
         )

        bronze_skills >> silver_skills

    with TaskGroup(group_id="extra_lakehouse") as extra_group:

        bronze_extra = spark_task(
            "bronze_extra",
            BRONZE_SCRIPT,
            args=["extra"]#, "--date", "2026-01-03"]
        )

        silver_extra = spark_task(
            "silver_extra",
            SILVER_SCRIPT,
            args=["extra"]
        )

        bronze_extra >> silver_extra

    # Dependências principais da DAG
    wait_vocation >> vocation_group
    wait_skills >> skills_group
    wait_extra >> extra_group


lakehouse = lakehouse_pipeline()

