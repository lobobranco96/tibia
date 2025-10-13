from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from datetime import datetime
import os
import io
import logging
import boto3
from botocore.client import Config
import pandas as pd

logger = logging.getLogger(__name__)

def selenium_webdriver():
  """
  Inicializa o driver do Selenium com Chromium em modo headless.

  Returns:
      webdriver.Chrome: instância configurada do driver.
  """
  chrome_options = Options()
  chrome_options.add_argument(
      "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/141.0.7390.65 Safari/537.36"
  )
  chrome_options.add_argument('--headless')
  chrome_options.add_argument('--no-sandbox')
  chrome_options.add_argument('--disable-dev-shm-usage')
  service = Service("/usr/local/share/chrome/chromedriver")
  return webdriver.Chrome(service=service, options=chrome_options)


class CSVBronze:
    """
    Classe responsável por salvar DataFrames como CSV no MinIO
    (camada Bronze do Data Lake), particionando por data.
    """

    def __init__(self, s3_endpoint, access_key, secret_key):

        if not all([s3_endpoint, access_key, secret_key]):
            raise ValueError(
                "S3 credentials missing: verifique S3_ENDPOINT, AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY"
            )

        self.endpoint_url = s3_endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        

        self.s3_client = boto3.client(
            's3',
            endpoint_url=s3_endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )

        today = datetime.today()
        self.year = today.strftime("%Y")
        self.month = today.strftime("%m")
        self.day = today.strftime("%d")

    def write(self, df, category, dataset_name, bucket_name="bronze"):
        """
        Escreve o DataFrame como CSV na camada Bronze do MinIO particionado por data.

        Args:
            df (pd.DataFrame): O DataFrame a ser salvo.
            dataset_name (str): Nome do dataset (sem extensão .csv).
            bucket_name (str): Nome do bucket (default: bronze).

        Returns:
            str: Caminho do arquivo salvo (URI s3a://...).
        """
        # Converte DataFrame em CSV na memória
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        key = (f"year={self.year}/"
              f"month={self.month}/"
              f"day={self.day}/"
              f"{category}/"
              f"{dataset_name}.csv"
              )

        # Upload no MinIO
        self.s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )

        logger.info(f"Arquivo salvo no MinIO: s3://{bucket_name}/{key}")
        return f"s3a://{bucket_name}/{key}"

def validate_highscore_csv(df: pd.DataFrame, expected_columns=None, min_rows=1) -> bool:
    """
    Valida um DataFrame de Highscore antes de passar para Silver.
    
    Args:
        df (pd.DataFrame): DataFrame a ser validado.
        expected_columns (list, optional): Lista de colunas esperadas.
        min_rows (int): Número mínimo de linhas esperado.

    Returns:
        bool: True se válido, False se inválido.
    """
    if df.empty:
        logger.warning("DataFrame está vazio.")
        return False

    if expected_columns:
        missing_cols = [col for col in expected_columns if col not in df.columns]
        if missing_cols:
            logger.warning(f"Colunas faltando: {missing_cols}")
            return False

    if len(df) < min_rows:
        logger.warning(f"Número de linhas insuficiente: {len(df)} < {min_rows}")
        return False

    return True