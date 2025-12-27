from datetime import datetime, timedelta
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
    start_date=datetime(2025, 12, 27),
    catchup=False,
    tags=["tibia", "extract"]
)
def landing_highscores_pipeline():

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
        vocation_done = EmptyOperator(task_id="vocation_done")

        extract_tasks >> vocation_done

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

        extract_tasks = [
            extract_axe(),
            extract_sword(),
            extract_magic(),
            extract_club(),
            extract_distance(),
            extract_shielding(),
            extract_fist()
        ]

        skills_done = EmptyOperator(task_id="skills_done")

        extract_tasks >> skills_done

    # ============================================================
    # TaskGroup: EXTRAÇÃO DE EXTRA
    # ============================================================
    with TaskGroup(group_id="extract_extra") as extract_extra_group:

        @task
        def extract_achievements():
            return extract_category("achievements")

        @task
        def extract_fishing():
            return extract_category("fishing")

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
            extract_fishing(),
            extract_loyalty(),
            extract_drome(),
            extract_boss(),
            extract_charm(),
            extract_goshnair(),
        ]

        extra_done = EmptyOperator(task_id="extra_done")

        extract_tasks >> extra_done

    # Dependências

    extract_vocation_group
    extract_skills_group
    extract_extra_group

landing = landing_highscores_pipeline()