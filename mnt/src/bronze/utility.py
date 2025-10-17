from datetime import datetime
import os
import io
import shutil
import logging
import boto3
from botocore.client import Config
import pandas as pd


# ==========================================================
#  CONFIG GLOBAL - Variáveis de ambiente e logger
# ==========================================================
logger = logging.getLogger(__name__)

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


class CSVBronze:
    """
    Classe responsável por salvar DataFrames como CSV no MinIO
    (camada Bronze do Data Lake), particionando por data.
    """

    def __init__(self):
        self.today = datetime.today()
        # Cria o client S3 global (MinIO)
        if not all([S3_ENDPOINT, ACCESS_KEY, SECRET_KEY]):
            raise ValueError("S3 credentials missing: verifique variáveis de ambiente.")

        self.s3_client = boto3.client(
            's3',
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )

    def write(self, df, category_dir, dataset_name, bucket_name="bronze",
              save_local_copy=True, delete_after_upload=True, local_path_base="/tmp"):
        """
        Escreve um DataFrame como CSV na camada Bronze do MinIO, particionado por data (year/month/day).

        O arquivo é salvo em um caminho particionado com base na data atual e pode ser salvo
        temporariamente no sistema de arquivos local antes de ser enviado ao MinIO. Após o upload,
        o arquivo local pode ser removido automaticamente, dependendo da configuração.

        Args:
            df (pd.DataFrame): O DataFrame a ser salvo.
            category_dir (str): Nome da subcategoria ou pasta lógica (ex: "experience", "skills").
            dataset_name (str): Nome do arquivo CSV (sem extensão).
            bucket_name (str, optional): Nome do bucket no MinIO. Default é "bronze".
            save_local_copy (bool, optional): Se True, salva o CSV localmente antes de enviar para o MinIO.
                                              Default é True.
            delete_after_upload (bool, optional): Se True, remove o arquivo/diretório local após o upload.
                                                  Só tem efeito se `save_local_copy` for True. Default é True.
            local_path_base (str, optional): Caminho base local para salvar arquivos temporários. Default é "/tmp".

        Returns:
            str: Caminho S3-style (`s3a://...`) do arquivo enviado ao MinIO.

        Raises:
            Exception: Qualquer erro durante a escrita local ou upload será capturado e logado.

        Exemplo:
            s3_path = writer.write(df, category_dir="experience", dataset_name="druid")
            # s3_path -> 's3a://bronze/year=2025/month=10/day=17/experience/druid.csv'
        """
        
        partition_path = f"year={self.today:%Y}/month={self.today:%m}/day={self.today:%d}"
        key = f"{partition_path}/{category_dir}/{dataset_name}.csv"
        local_csv_path = None

        if save_local_copy:
            # Salva localmente
            local_dir = os.path.join(local_path_base, partition_path, category_dir)
            os.makedirs(local_dir, exist_ok=True)
            local_csv_path = os.path.join(local_dir, f"{dataset_name}.csv")

            df.to_csv(local_csv_path, sep=";", index=False, encoding="utf-8")
            logger.info(f"CSV salvo localmente: {local_csv_path}")

            # Upload usando arquivo
            with open(local_csv_path, "rb") as f:
                self.s3_client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=f,
                    ContentType="text/csv"
                )

            # Limpeza opcional
            if delete_after_upload:
                try:
                    shutil.rmtree(os.path.join(local_path_base, f"year={self.today:%Y}"))
                    logger.info(f"Arquivo local removido: {local_csv_path}")
                except Exception as e:
                    logger.warning(f"Erro ao remover arquivo local: {e}")
        else:
            # Upload direto da memória
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, sep=";", index=False, encoding="utf-8")
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=csv_buffer.getvalue(),
                ContentType="text/csv"
            )

        logger.info(f"Arquivo salvo no MinIO: s3://{bucket_name}/{key}")
        return f"s3a://{bucket_name}/{key}"

def validate_csv(df: pd.DataFrame, expected_columns=None, min_rows=1) -> bool:
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