import io
import os
from datetime import datetime
import boto3
import logging
import json
import pandas as pd

#  CONFIG GLOBAL - Variáveis de ambiente e logger
logger = logging.getLogger(__name__)

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")

class ExtractionMetadataWriter:
    """
    Responsável por registrar metadados de extração na camada metadata.
    """

    def __init__(self):
        self.bucket = "lakehouse"
        self.prefix = "metadata/extraction_logs"
        self.today = datetime.today()

        self.s3 = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )

    def write(
        self,
        metadata: dict,
        pipeline: str,
        entity: str,
        subcategory: str,
        status: str = "SUCCESS"
    ):
        if not metadata:
            logger.warning("Metadata vazia — log não será gerado.")
            return

        partition = (
            f"year={self.today:%Y}/"
            f"month={self.today:%m}/"
            f"day={self.today:%d}"
        )

        timestamp = datetime.now().strftime("%H%M%S")

        key = (
            f"{self.prefix}/"
            f"{partition}/"
            f"{entity}/"
            f"{subcategory}_{timestamp}.json"
        )

        payload = {
            "pipeline": pipeline,
            "layer": "landing",
            "entity": entity,
            "subcategory": subcategory,
            "status": status,
            "bucket": metadata.get("bucket"),
            "data_key": metadata.get("key"),
            "rows": metadata.get("rows"),
            "columns": metadata.get("columns"),
            "extracted_at": metadata.get("timestamp"),
            "logged_at": datetime.now().isoformat()
        }

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(payload, ensure_ascii=False, indent=2),
            ContentType="application/json"
        )

        logger.info(f"Metadata registrada: s3://{self.bucket}/{key}")

#  CLASSE: CSVLanding
class CSVLanding:
    """
    Classe responsável por salvar DataFrames localmente na camada *Landing* do Data Lake,
    particionando por data (year/month/day).

    O padrão de diretórios segue a convenção:
    ```
    /mnt/minio/lakehouse/landing/year=YYYY/month=MM/day=DD/<category_dir>/<dataset_name>.csv
    ```

    Attributes:
        base_dir (str): Caminho base da camada Landing.
        today (datetime): Data atual para particionamento.
    """

    def __init__(self):
        self.today = datetime.today()
        self.bucket = "lakehouse"
        self.prefix = "landing"
        """
        Inicializa o gerenciador de escrita de CSVs na camada Landing.

        """
        self.s3 = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
            )
            
        self.metadata_writer = ExtractionMetadataWriter()

    def write(self, df: pd.DataFrame, 
            category_dir: str, 
            dataset_name: str, 
            pipeline: str = "landing_highscores_pipeline") -> dict:
            
        """
        Salva um DataFrame como CSV localmente na estrutura de partições por data.

        Args:
            df (pd.DataFrame): DataFrame a ser salvo.
            category_dir (str): Nome da subcategoria (ex: "experience", "skills").
            dataset_name (str): Nome do arquivo CSV (sem extensão).

        Returns:
            dict: Metadados do arquivo salvo contendo:
                - path (str): Caminho completo do CSV salvo.
                - rows (int): Número de linhas salvas.
                - columns (list): Lista de colunas do DataFrame.
                - timestamp (str): Data/hora do salvamento.

        Example:
            >>> writer = CSVLanding()
            >>> result = writer.write(df, category_dir="experience", dataset_name="druid")
            >>> print(result["path"])
            '/mnt/minio/lakehouse/landing/year=2025/month=11/day=10/experience/druid.csv'
        """
        if df is None or df.empty:
            logger.warning("Tentativa de salvar DataFrame vazio — operação cancelada.")
            return None

        partition = f"year={self.today:%Y}/month={self.today:%m}/day={self.today:%d}"

        timestamp = datetime.now().strftime("%H%M%S")   
        key = (
            f"{self.prefix}/"
            f"{partition}/"
            f"{category_dir}/"
            f"{dataset_name}_{timestamp}.csv"
        )

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, encoding="utf-8")

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="text/csv"
        )

        logger.info(f"Arquivo salvo na Landing: s3a://{self.bucket}/{key}")

        metadata =  {
          "bucket": self.bucket,
          "key": key,
          "rows": len(df),
          "columns": list(df.columns),
            "timestamp": datetime.now().isoformat()
        }

        # REGISTRO DE METADATA
        self.metadata_writer.write(
            metadata=metadata,
            pipeline=pipeline,
            entity=category_dir,
            subcategory=dataset_name,
            status="SUCCESS"
        )

        return metadata

#  FUNÇÃO: validate_csv
def validate_csv(df: pd.DataFrame, expected_columns=None, min_rows: int = 1) -> bool:
    """
    Valida um DataFrame antes de ser enviado à camada Landing.

    Regras:
    - Não pode estar vazio.
    - Deve conter todas as colunas esperadas (se informadas).
    - Deve ter pelo menos `min_rows` linhas.

    Args:
        df (pd.DataFrame): DataFrame a ser validado.
        expected_columns (list, optional): Lista de colunas esperadas.
        min_rows (int, optional): Número mínimo de linhas esperado. Default = 1.

    Returns:
        bool: `True` se válido, `False` caso contrário.

    Example:
        >>> valid = validate_csv(df, expected_columns=["Rank", "Name"], min_rows=10)
        >>> if valid:
        ...     print("Validação OK!")
    """
    if df is None or df.empty:
        logger.warning("DataFrame está vazio ou None.")
        return False

    if expected_columns:
        missing_cols = [col for col in expected_columns if col not in df.columns]
        if missing_cols:
            logger.warning(f"Colunas faltando no DataFrame: {missing_cols}")
            return False

    if len(df) < min_rows:
        logger.warning(f"Número de linhas insuficiente: {len(df)} < {min_rows}")
        return False

    logger.info(f"DataFrame validado com sucesso ({len(df)} linhas).")
    return True

