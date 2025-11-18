import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup 
import time
import random
from abc import ABC, abstractmethod

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

# CONSTRAINTS
DEFAULT_TIMEOUT = 2
DEFAULT_RETRIES = 1
DEFAULT_WAIT = 3
MAX_PAGES = 20


# /////////////////
# CLASSES AUXILIARES
# /////////////////
class BuscadorPagina:
    """
    Classe responsável por obter o HTML de uma página com controle de tentativas,
    tempo de espera e cabeçalhos HTTP personalizados.

    Aplica o princípio da responsabilidade única (SRP) — foca apenas na comunicação HTTP.
    """

    def __init__(self, headers=None, retries=DEFAULT_RETRIES, wait_time=DEFAULT_WAIT):
        """
        Inicializa o buscador de páginas.

        Args:
            headers (dict, opcional): Cabeçalhos HTTP para a requisição.
            retries (int): Número de tentativas em caso de falha.
            wait_time (int): Tempo (em segundos) de espera entre tentativas.
        """
        self.headers = headers or {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                " AppleWebKit/537.36 (KHTML, like Gecko)"
                " Chrome/91.0.4472.124 Safari/537.36"
            )
        }
        self.retries = retries
        self.wait_time = wait_time

    def get(self, url: str) -> bytes | None:
        """
        Realiza uma requisição HTTP GET com controle de tentativas e tempo de espera.

        Args:
            url (str): URL a ser acessada.

        Returns:
            bytes | None: Conteúdo HTML retornado pela requisição ou None em caso de falha.
        """
        for attempt in range(self.retries + 1):
            try:
                logger.info(f"Acessando URL: {url}")
                response = requests.get(url, headers=self.headers, timeout=DEFAULT_TIMEOUT)
                response.raise_for_status()
                return response.content
            except Exception as e:
                logger.warning(f"Erro ao acessar {url}: {e} (tentativa {attempt + 1})")
                if attempt < self.retries:
                    time.sleep(self.wait_time)
        return None

class HighScoreParser:
    """
    Classe responsável por interpretar o HTML de uma página de highscores do Tibia
    e convertê-lo em um DataFrame pandas.

    Atua como parser independente, seguindo o princípio da responsabilidade única (SRP).
    """

    def parse(self, html: bytes) -> pd.DataFrame:
        """
        Extrai os dados de uma tabela de highscores em formato HTML e retorna um DataFrame.

        Args:
            html (bytes): Conteúdo HTML da página.

        Returns:
            pd.DataFrame: Dados estruturados da tabela.
        """
        if not html:
            return pd.DataFrame()

        soup = BeautifulSoup(html, "lxml")
        table_container = soup.find("table", class_="Table3")

        if not table_container:
            logger.warning("Tabela 'Table3' não encontrada.")
            return pd.DataFrame()

        rows = table_container.find_all('tr')
        if len(rows) < 3:
            return pd.DataFrame()

        inner_table = rows[2].find("table", class_="TableContent")
        if not inner_table:
            return pd.DataFrame()

        table = inner_table.find_all('tr')
        if not table:
            return pd.DataFrame()

        columns = [col.get_text(strip=True) for col in table[0].find_all(['td', 'th'])]
        data = [
            [r.get_text(strip=True) for r in row.find_all(['td', 'th'])]
            for row in table[1:]
        ]

        logger.info(f"Extraídas {len(data)} linhas da tabela.")
        return pd.DataFrame(data, columns=columns)

# /////////////////
# CLASSE BASE ABSTRATA
# /////////////////

class BaseScraper(ABC):
    """
    Classe abstrata base para scrapers de highscores.
    Define a estrutura comum para as subclasses.
    """

    def __init__(self, fetcher: BuscadorPagina, parser: HighScoreParser):
        """
        Inicializa o scraper base com injeção de dependências.

        Args:
            fetcher (BuscadorPagina): Classe responsável pela obtenção do HTML.
            parser (HighScoreParser): Classe responsável por transformar HTML em DataFrame.
        """
        self.fetcher = fetcher
        self.parser = parser

    def processar_paginas(self, lista_paginas: list[str], wait_random: bool = True) -> pd.DataFrame:
        """
        Percorre uma lista de URLs, faz o download do HTML, processa e concatena os resultados.

        Args:
            lista_paginas (list[str]): Lista de URLs a serem processadas.
            wait_random (bool): Define se haverá tempo de espera aleatório entre as páginas.

        Returns:
            pd.DataFrame: Dados consolidados de todas as páginas processadas.
        """
        dataframe = pd.DataFrame()

        for pagina in lista_paginas:
            html = self.fetcher.get(pagina)
            df = self.parser.parse(html)
            if not df.empty:
                dataframe = pd.concat([dataframe, df], ignore_index=True)
            else:
                logger.warning(f"Nenhum dado extraído da página {pagina}.")

            if wait_random:
                time.sleep(random.uniform(1, 3))

        logger.info(f"Total de linhas processadas: {len(dataframe)}")
        return dataframe

    @abstractmethod
    def get_data(self, *args, **kwargs) -> pd.DataFrame:
        """
        Método abstrato a ser implementado pelas subclasses.

        Deve retornar um DataFrame com os dados extraídos.
        """
        pass

# /////////////////
# VOCATION SCRAPER
# /////////////////

class VocationScraper(BaseScraper):
    """
    Realiza o scraping das tabelas de Highscores de vocações no Tibia.com
    para diferentes tipos de mundos (Open, Optional, Retro, etc.).
    """

    BASE_URL = (
        "https://www.tibia.com/community/?subtopic=highscores&world="
        "&beprotection=-1"
        "&worldtypes[]={world_type}"
        "&category=6"
        "&profession={vocation_id}"
        "&currentpage={page_number}"
    )

    WORLD_TYPES = {
        "Open PvP": 0,
        "Optional PvP": 1,
        "Hardcore PvP": 2,
        "Retro Open PvP": 3,
        "Retro Hardcore PvP": 4,
    }

    def get_data(self, vocation_id: int) -> pd.DataFrame:
        """
        Coleta os dados de uma vocação específica em todos os tipos de mundo disponíveis.

        Args:
            vocation_id (int): Identificador numérico da vocação (1–6 no Tibia).

        Returns:
            pd.DataFrame: DataFrame consolidado com os dados de todas as páginas e mundos.
        """

        max_pages = MAX_PAGES
        todos_os_dados = []
        if vocation_id == 1:
          max_pages = 10
        for world_name, world_type in self.WORLD_TYPES.items():
            urls = [
                self.BASE_URL.format(world_type=world_type, vocation_id=vocation_id, page_number=i)
                for i in range(1, max_pages + 1)
            ]
            logger.info(f"Coletando dados: {world_name} | Vocação {vocation_id}")
            df = self.processar_paginas(urls)

            if not df.empty:
                df["WorldType"] = world_name
                todos_os_dados.append(df)

        return pd.concat(todos_os_dados, ignore_index=True) if todos_os_dados else pd.DataFrame()



# /////////////////
# CATEGORY SCRAPER
# /////////////////

class CategoryScraper(BaseScraper):
    """
    Realiza o scraping dos highscores de categorias (skills e extras) do Tibia.com.

    Cada categoria pode representar habilidades como Sword Fighting, Magic Level, etc.
    """

    BASE_URL = (
        "https://www.tibia.com/community/?subtopic=highscores&world="
        "&beprotection=-1"
        "&category={category_id}"
        "&profession={vocation_id}"
        "&currentpage={page_number}"
    )

    DEFAULT_CATEGORIES = {
            "achievements": (1, "Achievements", 0),
            "axe": (2, "Axe Fighting", 2),
            "sword": (13, "Sword Fighting", 2),
            "club": (4, "Club Fighting", 2),
            "distance": (5, "Distance Fighting", 3),
            "magic_level": (11, "Magic Level", 4),
            "fist": (8, "Fist Fighting", 0),
            "shielding": (12, "Shielding", 0),
            "fishing": (7, "Fishing", 0),
            "loyalty": (10, "Loyalty Points", 0),
            "drome": (14, "Drome Score", 0),
            "boss": (15, "Boss Points", 0),
            "charm": (3, "Charm Points", 0),
            "goshnair": (9, "Goshnar's Taint", 0)
    }

    def __init__(self, fetcher: BuscadorPagina, parser: HighScoreParser, categories=None):
        """
        Inicializa o scraper de categorias com dicionário de mapeamento configurável.

        Args:
            fetcher (BuscadorPagina): Instância responsável por obter o HTML.
            parser (HighScoreParser): Instância responsável por converter HTML em DataFrame.
            categories (dict, opcional): Mapeamento personalizado de categorias.
        """
        super().__init__(fetcher, parser)
        self.categories = categories or self.DEFAULT_CATEGORIES

    def get_data(self, category_key: str) -> pd.DataFrame:
        """
        Coleta os dados de uma categoria específica (skill ou extra).

        Args:
            category_key (str): Nome da categoria (ex: "sword", "magic", "fishing").

        Returns:
            pd.DataFrame: DataFrame com os dados extraídos da categoria.
        """
        if category_key not in self.categories:
            raise ValueError(f"Categoria inválida: {category_key}")

        category_id, category_name, vocation_id = self.categories[category_key]
        urls = [
            self.BASE_URL.format(
                category_id=category_id, vocation_id=vocation_id, page_number=i
            ) for i in range(1, MAX_PAGES + 1)
        ]

        df = self.processar_paginas(urls)
        if not df.empty:
            df["Category"] = category_name
        return df
