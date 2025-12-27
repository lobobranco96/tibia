import logging
from .utility import CSVLanding, validate_csv
from .scraper import BuscadorPagina, HighScoreParser, VocationScraper, CategoryScraper


# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# Inicialização da Classe CSVLanding
landing_data = CSVLanding()


# Função: Extração por Vocação
def extract_vocation(vocation: str) -> str:
    """
    Extrai highscores de uma vocação específica e envia para a camada 'landing'.

    Args:
        vocation (str): Nome da vocação (none, knight, paladin, sorcerer, druid, monk)

    Returns:
        str: Caminho no MinIO onde o arquivo foi salvo ou None se falhar.
    """
    try:
        # Mapeamento entre nome e ID das vocações no Tibia
        valid_vocations = {
            "none": 1,
            "knight": 2,
            "paladin": 3,
            "sorcerer": 4,
            "druid": 5,
            "monk": 6,
        }

        vocation = vocation.lower().strip()
        vocation_id = valid_vocations.get(vocation)

        if vocation_id is None:
            logger.warning(
                "Vocação inválida. Use: none, knight, paladin, sorcerer, druid ou monk."
            )
            return None

        # Cria instâncias de dependências (injeção manual)
        fetcher = BuscadorPagina()
        parser = HighScoreParser()
        scraper = VocationScraper(fetcher, parser)

        # Executa o scraping
        df = scraper.get_data(vocation_id)

        if df.empty:
            raise ValueError("Nenhum dado retornado para a vocação especificada.")

         # Validação de schema esperado
        expected_columns = ["Rank", "Name", "Vocation", "World", "Level", "Points", "WorldType"]
        if not validate_csv(df, expected_columns=expected_columns):
            raise ValueError(f"Validação falhou para vocação '{vocation}'")

        # Armazena no MinIO (ou camada landing)
        logger.info(f"Extração concluída para vocação '{vocation}'. Enviando ao MinIO...")
        category_dir = "experience"
        dataset_name = vocation

        return landing_data.write(df, category_dir, dataset_name)

    except Exception as e:
        logger.error(f"Erro durante extração de vocação '{vocation}': {e}", exc_info=True)
        return None



# Função: Extração por Categoria

def extract_category(category: str) -> str:
    """
    Extrai highscores de uma categoria (extra ou skills) e salva como CSV na landing.

    Args:
        category (str): Nome da categoria (fishing, sword, distance, magic, shielding, etc.)

    Returns:
        str: Caminho no MinIO onde o arquivo foi salvo ou None se falhar.
    """
    try:
        # Categorias válidas
        valid_extra = [
            "achievements", "fishing", "loyalty", "drome", "boss", "charm", "goshnair"
        ]

        valid_skills = [
            "axe", "sword", "club", "distance", "magic_level", "fist", "shielding"
        ]

        category = category.lower().strip()

        # Define diretório de destino
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

        # Cria instâncias de dependências (injeção manual)
        fetcher = BuscadorPagina()
        parser = HighScoreParser()
        scraper = CategoryScraper(fetcher, parser)

        logger.info(f"Extraindo categoria '{category}' ({category_dir})...")

        # Executa o scraping da categoria
        df = scraper.get_data(category)

        if df.empty:
            raise ValueError("Nenhum dado retornado para a categoria especificada.")

        # Validação opcional (depende do HTML)
        if not validate_csv(df):
            raise ValueError(f"Validação falhou para categoria '{category}'")

        # Grava na camada landing
        dataset_name = category
        return landing_data.write(df, category_dir, dataset_name)

    except Exception as e:
        logger.error(f"Erro durante extração da categoria '{category}': {e}", exc_info=True)
        return None

