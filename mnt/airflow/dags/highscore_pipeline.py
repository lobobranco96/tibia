import os
from datetime import datetime, timedelta
from src.bronze_app import extract_vocation, extract_category

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "lobobranco",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}
def highscore_pipeline():

  with TaskGroup("extract") as extract_group:
      none_task = extract_vocation('none')
      knight_task = extract_vocation('knight')
      paladin_task = extract_vocation('paladin')
      sorcerer_task = extract_vocation('sorcerer')
      druid_task = extract_vocation('druid')
      monk_task = extract_vocation('monk')

      [none_task, knight_task, paladin_task, sorcerer_task, druid_task, monk_task]

highscore = highscore_pipeline()