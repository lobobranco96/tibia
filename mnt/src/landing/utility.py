from datetime import datetime
import os
import logging
import pandas as pd

#  CONFIG GLOBAL - Variáveis de ambiente e logger
logger = logging.getLogger(__name__)



#  CLASSE: CSVLanding
class CSVLanding:
    """
    Classe responsável por salvar DataFrames localmente na camada *Landing* do Data Lake,
    particionando por data (year/month/day).

    O padrão de diretórios segue a convenção:
    ```
    /mnt/minio/landing/year=YYYY/month=MM/day=DD/<category_dir>/<dataset_name>.csv
    ```

    Attributes:
        base_dir (str): Caminho base da camada Landing.
        today (datetime): Data atual para particionamento.
    """

    def __init__(self, base_dir: str = "/mnt/minio/landing"):
        """
        Inicializa o gerenciador de escrita de CSVs na camada Landing.

        Args:
            base_dir (str, optional): Diretório base onde os arquivos serão salvos.
                Padrão: "/mnt/minio/landing".
        """
        self.today = datetime.today()
        self.base_dir = base_dir

    def write(self, df: pd.DataFrame, category_dir: str, dataset_name: str) -> dict:
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
            '/mnt/minio/landing/year=2025/month=11/day=10/experience/druid.csv'
        """
        if df is None or df.empty:
            logger.warning("Tentativa de salvar DataFrame vazio — operação cancelada.")
            return None

        partition = f"year={self.today:%Y}/month={self.today:%m}/day={self.today:%d}"
        landing_path = os.path.join(self.base_dir, partition, category_dir)
        os.makedirs(landing_path, exist_ok=True)

        file_path = os.path.join(landing_path, f"{dataset_name}.csv")

        # Salva o arquivo CSV
        df.to_csv(file_path, index=False, encoding="utf-8")
        logger.info(f"Arquivo salvo na camada Landing: {file_path} "
                    f"({len(df)} linhas, {len(df.columns)} colunas)")

        return {
            "path": file_path,
            "rows": len(df),
            "columns": df.columns.tolist(),
            "timestamp": datetime.now().isoformat(),
        }



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
