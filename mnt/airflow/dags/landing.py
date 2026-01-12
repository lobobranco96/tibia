from datetime import datetime, timedelta
import os
import boto3
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from src.landing.landing_app import extract_vocation, extract_category

default_args = {
    "owner": "lobobranco",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="landing_highscores_pipeline",
    description="Pipeline de extração de Pagina highscores do Tibia",
    default_args=default_args,
    start_date=datetime(2025, 10, 15),
    catchup=False,
    tags=["tibia", "extract", "landing"]
)
def landing_highscores_pipeline():

    @task
    def write_success_file(group_type):
        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )

        today = datetime.today()
        partition = f"year={today:%Y}/month={today:%m}/day={today:%d}"

        key = f"landing/{partition}/{group_type}/_SUCCESS"

        s3.put_object(
            Bucket="lakehouse",
            Key=key,
            Body=""
        )

        return f"s3://lakehouse/{key}"

    # TaskGroup: EXTRAÇÃO DE VOCAÇÕES
    with TaskGroup(group_id="extract_vocation") as extract_vocation_group:

        @task
        def extract_none():
            return extract_vocation("none")

        @task
        def extract_knight():
            return extract_vocation("knight")

        @task
        def extract_paladin():
            return extract_vocation("paladin")

        @task
        def extract_sorcerer():
            return extract_vocation("sorcerer")

        @task
        def extract_druid():
            return extract_vocation("druid")

        @task
        def extract_monk():
            return extract_vocation("monk")

        extract_tasks = [
            extract_none(),
            extract_knight(),
            extract_paladin(),
            extract_sorcerer(),
            extract_druid(),
            extract_monk(),
        ]

        success = write_success_file("experience")
        extract_tasks >> success


    # TaskGroup: EXTRAÇÃO DE SKILLS
    with TaskGroup(group_id="extract_skills") as extract_skills_group:

        @task
        def extract_axe():
            return extract_category("axe")

        @task
        def extract_sword():
            return extract_category("sword")

        @task
        def extract_magic():
            return extract_category("magic_level")

        @task
        def extract_club():
            return extract_category("club")

        @task
        def extract_distance():
            return extract_category("distance")

        @task
        def extract_shielding():
            return extract_category("shielding")

        @task
        def extract_fist():
            return extract_category("fist")

        @task
        def extract_fishing():
            return extract_category("fishing")

        extract_tasks = [
            extract_axe(),
            extract_sword(),
            extract_magic(),
            extract_club(),
            extract_distance(),
            extract_shielding(),
            extract_fist(),
            extract_fishing(),
        ]

        success = write_success_file("skills")
        extract_tasks >> success

    # TaskGroup: EXTRAÇÃO DE EXTRA
    with TaskGroup(group_id="extract_extra") as extract_extra_group:

        @task
        def extract_achievements():
            return extract_category("achievements")

        @task
        def extract_loyalty():
            return extract_category("loyalty")

        @task
        def extract_drome():
            return extract_category("drome")

        @task
        def extract_boss():
            return extract_category("boss")

        @task
        def extract_charm():
            return extract_category("charm")

        @task
        def extract_goshnair():
            return extract_category("goshnair")

        extract_tasks = [
            extract_achievements(),
            extract_loyalty(),
            extract_drome(),
            extract_boss(),
            extract_charm(),
            extract_goshnair(),
        ]

        success = write_success_file("extra")
        extract_tasks >> success

    # Dependências
    extract_vocation_group
    extract_skills_group
    extract_extra_group



landing = landing_highscores_pipeline()
