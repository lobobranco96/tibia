from datetime import datetime
import os
import io
import shutil
import logging
import pandas as pd


# ==========================================================
#  CONFIG GLOBAL - Variáveis de ambiente e logger
# ==========================================================
logger = logging.getLogger(__name__)


class CSVLanding:
    """
    Classe responsável por salvar DataFrames localmente na camada Landing do Data Lake,
    particionando por data (year/month/day).
    """

    def __init__(self, base_dir="/mnt/minio/landing"):
        self.today = datetime.today()
        self.base_dir = base_dir

    def write(self, df, category_dir, dataset_name, delete_old=True):
        """
        Salva um DataFrame como CSV localmente na estrutura de partições de data.

        Args:
            df (pd.DataFrame): DataFrame a ser salvo.
            category_dir (str): Nome da subcategoria (ex: "experience", "skills").
            dataset_name (str): Nome do arquivo CSV (sem extensão).
            delete_old (bool): Se True, apaga versões antigas do mesmo dataset no mesmo dia.

        Returns:
            str: Caminho completo do arquivo CSV salvo.
        """
        """
        Exemplo:
            s3_path = writer.write(df, category_dir="experience", dataset_name="druid")
            # s3_path -> 's3a://landing/year=2025/month=10/day=17/experience/druid.csv'
        """
        
        partition_path = os.path.join(
            self.base_dir,
            f"year={self.today:%Y}",
            f"month={self.today:%m}",
            f"day={self.today:%d}",
            category_dir
        )
        
        os.makedirs(partition_path, exist_ok=True)

        full_path = os.path.join(partition_path, f"{dataset_name}.csv")

        # (Opcional) Remove CSVs antigos da mesma categoria/dataset no mesmo dia
        if delete_old:
            for fname in os.listdir(partition_path):
                if fname.startswith(dataset_name) and fname.endswith(".csv"):
                    try:
                        os.remove(os.path.join(partition_path, fname))
                    except Exception as e:
                        logger.warning(f"Falha ao remover arquivo antigo: {fname} - {e}")

        # Salva o CSV
        df.to_csv(full_path, sep=";", index=False, encoding="utf-8")
        logger.info(f"Arquivo salvo na camada Landing: {full_path}")

        return {
          "path": full_path,
          "rows": len(df),
          "columns": df.columns.tolist(),
          "timestamp": datetime.now().isoformat()
              }

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