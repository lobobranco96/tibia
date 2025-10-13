import os
from datetime import datetime, timedelta
from src.utility import CSVBronze, validate_highscore_csv
from src.extract import HighscoreVocation

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

def upload(vocation: str, category: str) -> str:
    # Config MinIO
    s3_endpoint = os.getenv("S3_ENDPOINT")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    valid_vocations = {
        'none': 'no_vocation',
        'knight': 'knight',
        'paladin': 'paladin',
        'sorcerer': 'sorcerer',
        'druid': 'druid',
        'monk': 'monk',
    }
    highscore = HighscoreVocation()

    method_name = valid_vocations.get(vocation.lower())
    if method_name and hasattr(highscore, method_name):
      method = getattr(highscore, method_name)
      df = method()

      # Validação mínima antes do upload
      expected_columns = ["Rank", "Name", "Vocation", "World", "Level", "Points", "WorldType"]
      if not validate_highscore_csv(df, expected_columns=expected_columns):
          raise ValueError(f"Validação falhou para {method_name}")

      minio = CSVBronze(s3_endpoint, access_key, secret_key)
      return minio.write(df, method_name, category, bucket_name="bronze")
    else:
      logger.info("Vocação inválida. Use: none, knight, paladin, sorcerer, druid ou monk.")
      return None

default_args = {
    "owner": "lobobranco",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}
def highscore_pipeline():

  @task
  def none():
      return upload('none', "level")

  @task
  def knight():
      return upload('knight', "level")

  @task
  def paladin():
      return upload('paladin', "level")

  @task
  def sorcerer():
      return upload('sorcerer', "level")

  @task
  def druid():
      return upload('druid', "level")

  @task
  def monk():
      return upload('monk', "level")

  with TaskGroup("extract") as extract_group:
      none_task = none()
      knight_task = knight()
      paladin_task = paladin()
      sorcerer_task = sorcerer()
      druid_task = druid()
      monk_task = monk()

      [none, knight, paladin, sorcerer, druid, monk]

highscore = highscore_pipeline()