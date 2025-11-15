from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# SCRIPTS PATH
BRONZE_SCRIPT = {
    "vocation": ["/opt/airflow/dags/src/jobs/bronze/vocation.py"],
    "skills": ["/opt/airflow/dags/src/jobs/bronze/skills.py"],
    "extra": ["/opt/airflow/dags/src/jobs/bronze/extra.py"]
}
SILVER_SCRIPT = {
    "vocation": ["/opt/airflow/dags/src/jobs/silver/vocation.py"],
    "skills": ["/opt/airflow/dags/src/jobs/silver/skills.py"],
    "extra": ["/opt/airflow/dags/src/jobs/silver/extra.py"]
}


default_args = {
    "owner": "lobobranco",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def spark_task(task_id, app_path, args=None):
    """
    Cria um SparkSubmitOperator configurado para executar um script Spark com argumentos.
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
        conn_id="spark_default",
        conf=conf,
        verbose=True,
        application_args=[args] if args else None
    )

@dag(
    dag_id="lakehouse_pipeline",
    description="Lakehouse pipeline, inicia o processamento dos dados da camada Bronze até Silver",
    default_args=default_args,
    start_date=datetime(2025, 10, 28),
    catchup=False,
    tags=["tibia", "lakehouse", "etl"]
)
def lakehouse_pipeline():

    # Sensor que espera a DAG Landing finalizar
    wait_for_landing = ExternalTaskSensor(
        task_id="wait_for_landing_dag",
        external_dag_id="tibia_highscores_pipeline",
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60 * 3,
        soft_fail=False,
    )

    # Tasks Bronze
    bronze_vocation = spark_task("bronze_vocation", BRONZE_SCRIPT["vocation"][0])
    bronze_skills = spark_task("bronze_skills", BRONZE_SCRIPT["skills"][0])
    bronze_extra = spark_task("bronze_extra", BRONZE_SCRIPT["extra"][0])

    bronze_tasks = [bronze_vocation, bronze_skills, bronze_extra]

    # Tasks Silver
    silver_vocation = spark_task("silver_vocation", SILVER_SCRIPT["vocation"][0])
    silver_skills = spark_task("silver_skills", SILVER_SCRIPT["skills"][0])
    silver_extra = spark_task("silver_extra", SILVER_SCRIPT["extra"][0])

    silver_tasks = [silver_vocation, silver_skills, silver_extra]

    # Dependência
    #wait_for_landing >> [*bronze_tasks] >> [*silver_tasks]
    wait_for_landing >> bronze_tasks

    for task in bronze_tasks:
        task >> silver_tasks


lakehouse = lakehouse_pipeline()