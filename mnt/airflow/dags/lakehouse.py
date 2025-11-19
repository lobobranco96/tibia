from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


BRONZE_SCRIPT = {
    "vocation": "/opt/airflow/dags/src/jobs/bronze/vocation.py",
    "skills": "/opt/airflow/dags/src/jobs/bronze/skills.py",
    "extra": "/opt/airflow/dags/src/jobs/bronze/extra.py",
}
SILVER_SCRIPT = {
    "vocation": "/opt/airflow/dags/src/jobs/silver/vocation.py",
    "skills": "/opt/airflow/dags/src/jobs/silver/skills.py",
    "extra": "/opt/airflow/dags/src/jobs/silver/extra.py",
}

default_args = {
    "owner": "lobobranco",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def spark_task(task_id, app_path, args=None):
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
        pool="spark_pool",
        conn_id="spark_default",
        conf=conf,
        verbose=True,
        application_args=[args] if args else None,
    )


@dag(
    dag_id="lakehouse_pipeline",
    description="Lakehouse pipeline, inicia o processamento dos dados da camada Bronze até Silver",
    default_args=default_args,
    start_date=datetime(2025, 11, 17),
    tags=["tibia", "lakehouse", "etl"]
)
def lakehouse_pipeline():

    wait_for_landing = ExternalTaskSensor(
        task_id="wait_for_landing_dag",
        external_dag_id="tibia_highscores_pipeline",
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60 * 3,
        soft_fail=False,
    )

    start_pipeline = BashOperator(
        task_id="start_pipeline",
        bash_command="echo 'Iniciando a pipeline do lakehouse.'"
    )

    end_pipeline = BashOperator(
        task_id="end_pipeline",
        bash_command="echo 'Finalizando a pipeline.'"
    )

    # Task Group
    with TaskGroup(group_id="lakehouse") as lakehouse_group:

        # Tasks Bronze
        bronze_vocation = spark_task("bronze_vocation", BRONZE_SCRIPT["vocation"])
        bronze_skills = spark_task("bronze_skills", BRONZE_SCRIPT["skills"])
        bronze_extra = spark_task("bronze_extra", BRONZE_SCRIPT["extra"])

        bronze_tasks = [bronze_vocation, bronze_skills, bronze_extra]

        # Tasks Silver
        silver_vocation = spark_task("silver_vocation", SILVER_SCRIPT["vocation"])
        silver_skills = spark_task("silver_skills", SILVER_SCRIPT["skills"])
        silver_extra = spark_task("silver_extra", SILVER_SCRIPT["extra"])

        silver_tasks = [silver_vocation, silver_skills, silver_extra]

        # bronze[i] → silver[i]
        for b_task, s_task in zip(bronze_tasks, silver_tasks):
            b_task >> s_task

    # Dependências externas
    wait_for_landing >> start_pipeline >> lakehouse_group >> end_pipeline


lakehouse = lakehouse_pipeline()