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
GOLD_SCRIPT = "/opt/airflow/dags/src/jobs/gold_job.py"

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
    - Inclusão de .jars obrigatórias para o funcionamento do iceberg/aws/nessie
    """
    conf = {
        "spark.jars": ",".join([
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
            "/opt/spark/jars/hadoop-aws-3.3.4.jar",
            "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
            "/opt/spark/jars/nessie-spark-extensions-3.5_2.12-0.99.0.jar",
            "/opt/spark/jars/iceberg-aws-bundle-1.6.1.jar",
            "/opt/spark/jars/bundle-2.28.13.jar"
        ]),

        # LIMITAÇÃO DE RECURSOS
        "spark.executor.instances": "1",
        "spark.executor.cores": "1",
        "spark.executor.memory": "512m",
        "spark.executor.memoryOverhead": "512m",
        "spark.driver.memory": "512m",

        # PARA ICEBERG + SMALL DATA
        "spark.sql.shuffle.partitions": "8",
        "spark.default.parallelism": "8",

    }

    return SparkSubmitOperator(
        task_id=task_id,
        application=app_path,
        pool="spark_pool",
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
    start_date=datetime(2025, 1, 5),
    tags=["tibia", "lakehouse", "etl"]
)
def lakehouse_pipeline():
    """
    DAG responsável por orquestrar o processamento do Lakehouse Tibia
    para os domínios de vocation, skills e extra, contemplando as camadas
    Bronze e Silver.

    Fluxo da pipeline:
        - Aguarda a disponibilidade dos dados brutos da camada Landing
          no Data Lake (MinIO/S3), utilizando S3KeySensor para verificar
          a existência do arquivo _SUCCESS de cada domínio:
              • vocation
              • skills
              • extra

        - Após a confirmação dos dados de Landing, cada domínio é processado
          de forma independente e paralela.

        - Para cada domínio:
            • Executa a camada Bronze, responsável pela leitura dos dados brutos,
              padronização de schema e persistência no formato Iceberg.
            • Em seguida, executa a camada Silver, aplicando regras de negócio,
              refinamento dos dados e controle histórico com versionamento
              baseado no padrão SCD Type 2.

        - As camadas Bronze e Silver são encadeadas dentro de TaskGroups,
          garantindo organização, isolamento por domínio e melhor visualização
          da DAG no Airflow.

    O processamento é totalmente distribuído via Apache Spark e estruturado
    segundo a arquitetura Medallion (Bronze → Silver), utilizando Apache Iceberg
    como formato de tabela e Nessie como catálogo e controle de versões.
    """

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
             args=["skills"]#, "--date", "2026-01-03"]
         )

        silver_skills = spark_task(
             "silver_skills",
             SILVER_SCRIPT,
             args=["skills"]
         )

        bronze_skills >> silver_skills

    gold_start_checkpoint = BashOperator(
        task_id="gold_start_checkpoint",
        bash_command="""
         "Bronze e Silver finalizados. Iniciando camada Gold..."   
        date
        """
    )

    gold_job = spark_task(
        "Gold_Layer",
        GOLD_SCRIPT)


    # Dependências principais da DAG
    wait_vocation >> vocation_group
    wait_skills >> skills_group

    [vocation_group, skills_group] >> gold_start_checkpoint
    gold_start_checkpoint >> gold_job


lakehouse = lakehouse_pipeline()



