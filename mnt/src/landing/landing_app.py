import logging
from .utility import CSVLanding, validate_csv
from .extract import Vocation, Category

# ================================================================
# Configuração de Logging
# ================================================================
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ================================================================
# Inicialização da Classe CSVLanding
# ================================================================
landing_data = CSVLanding()

# ================================================================
# Função: Extração por Vocação
# ================================================================
def extract_vocation(vocation: str) -> str:
    """
    Extrai highscores de uma vocação específica e envia para a camada 'landing'.

    Args:
        vocation (str): Nome da vocação (none, knight, paladin, sorcerer, druid, monk)

    Returns:
        str: Caminho no MinIO onde o arquivo foi salvo ou None se falhar.
    """
    try:
        valid_vocations = {
            "none": "no_vocation",
            "knight": "knight",
            "paladin": "paladin",
            "sorcerer": "sorcerer",
            "druid": "druid",
            "monk": "monk",
        }

        vocation = vocation.lower().strip()
        highscore = Vocation()

        method_name = valid_vocations.get(vocation)
        if method_name and hasattr(highscore, method_name):
            method = getattr(highscore, method_name)
            df = method()

            expected_columns = ["Rank", "Name", "Vocation", "World", "Level", "Points", "WorldType"]
            if not validate_csv(df, expected_columns=expected_columns):
                raise ValueError(f"Validação falhou para {method_name}")



            logger.info(f"Extração concluída para vocação '{vocation}'. Enviando ao MinIO...")
            category_dir = "experience"
            dataset_name = method_name
            
            return landing_data \
            .write(df,
            category_dir,
            dataset_name)

        else:
            logger.warning("Vocação inválida. Use: none, knight, paladin, sorcerer, druid ou monk.")
            return None

    except Exception as e:
        logger.error(f"Erro durante extração de vocação '{vocation}': {e}", exc_info=True)
        return None


# ================================================================
# Função: Extração por Categoria
# ================================================================
def extract_category(category: str) -> str:
    """
    Extrai highscores de uma categoria (extra ou skills) e salva como CSV na landing.

    Args:
        category (str): Nome da categoria (achievements, fishing, axe, sword, etc.)

    Returns:
        str: Caminho local onde o arquivo foi salvo ou None se falhar.
    """
    try:
        valid_extra = [
            "achievements", "fishing", "loyalty", "drome", "boss", "charm", "goshnair"
        ]

        valid_skills = [
            "axe", "sword", "club", "distance", "magic_level", "fist", "shielding"
        ]

        category = category.lower().strip()
        highscore = Category()

        if category in valid_extra:
            category_dir = "extra"
        elif category in valid_skills:
            category_dir = "skills"
        else:
            logger.warning(
                f"Categoria inválida: {category}. "
                "Use uma das seguintes opções:\n"
                "Extras: achievements, fishing, loyalty, drome, boss, charm, goshnair\n"
                "Skills: axe, sword, club, distance, magic_level, fist, shielding"
            )
            return None

        logger.info(f"Extraindo categoria '{category}' ({category_dir})...")

        df = highscore.get_by_category(category)
        dataset_name = category

        return landing_data \
            .write(df,
            category_dir,
            dataset_name)
        
    except Exception as e:
        logger.error(f"Erro durante extração da categoria '{category}': {e}", exc_info=True)
        return None
