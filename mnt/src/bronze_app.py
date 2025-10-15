import os
from bronze.utility import CSVBronze, validate_highscore_csv
from bronze.extract import Vocation, Skills
import logging

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

def extract_vocation(vocation: str) -> str:
    # Config MinIO
    category = 'experience'
    valid_vocations = {
        'none': 'no_vocation',
        'knight': 'knight',
        'paladin': 'paladin',
        'sorcerer': 'sorcerer',
        'druid': 'druid',
        'monk': 'monk',
    }
    highscore = Vocation()

    method_name = valid_vocations.get(vocation.lower())
    if method_name and hasattr(highscore, method_name):
      method = getattr(highscore, method_name)
      df = method()

      # Validação mínima antes do upload
      expected_columns = ["Rank", "Name", "Vocation", "World", "Level", "Points", "WorldType"]
      if not validate_highscore_csv(df, expected_columns=expected_columns):
          raise ValueError(f"Validação falhou para {method_name}")

      minio = CSVBronze()
      return minio.write(df, method_name, category, bucket_name="bronze")
    else:
      logger.info("Vocação inválida. Use: none, knight, paladin, sorcerer, druid ou monk.")
      return None

def extract_skill():
  pass