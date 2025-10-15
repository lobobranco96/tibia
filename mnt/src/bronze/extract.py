import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import random

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

class HighScore:
    """
    Classe responsável por realizar o scraping de dados da tabela de Highscores
    no site oficial do Tibia. Utiliza `requests` para obter o HTML e `BeautifulSoup`
    para extrair o conteúdo tabular.
    """

    def __init__(self):
        """Inicializa o scraper de highscores."""
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        logger.info("HighScore scraper inicializado.")

    def scraper(self, url):
        """
        Realiza o scraping de uma única página de highscores e retorna os dados em formato DataFrame.

        Faz uma requisição HTTP à página especificada, identifica a tabela principal contendo
        as informações de ranking e converte o conteúdo em um DataFrame do pandas.

        Args:
            url (str): URL da página de highscores a ser processada.

        Returns:
            pandas.DataFrame: DataFrame contendo as colunas extraídas da tabela.
            Caso a página não possua dados válidos, retorna um DataFrame vazio.
        """
        logger.info(f"Acessando URL: {url}")
        try:
            response = requests.get(url, headers=self.headers, timeout=1)
            soup = BeautifulSoup(response.content, "lxml")

            table_container = soup.find("table", class_="Table3")
            if not table_container:
                logger.warning("Tabela 'Table3' não encontrada.")
                return pd.DataFrame()

            rows = table_container.find_all('tr')
            if len(rows) < 3:
                logger.warning("Menos de 3 linhas na tabela principal.")
                return pd.DataFrame()

            content = rows[2]
            inner_table = content.find("table", class_="TableContent")
            if not inner_table:
                logger.warning("Tabela 'TableContent' não encontrada.")
                return pd.DataFrame()

            table = inner_table.find_all('tr')
            if not table:
                logger.warning("Nenhuma linha encontrada na tabela interna.")
                return pd.DataFrame()

            columns = [col.get_text(strip=True) for col in table[0].find_all(['td', 'th'])]
            data = []
            for row in table[1:]:
                values = [r.get_text(strip=True) for r in row.find_all(['td', 'th'])]
                data.append(values)

            logger.info(f"Extraídas {len(data)} linhas da tabela.")
            return pd.DataFrame(data, columns=columns)

        except Exception as e:
            logger.error(f"Erro ao processar a página {url}: {e}")
            return pd.DataFrame()



class Vocation:
    """
    Classe que encapsula o scraping das tabelas de highscores por vocação e tipo de mundo no Tibia.

    Esta classe automatiza a coleta de rankings de experiência de jogadores, divididos por vocação
    e tipo de servidor (Open PvP, Optional PvP, etc.), retornando os resultados em DataFrames.

    Attributes:
        table (HighScore): Instância da classe HighScore usada para extrair os dados de cada página.
        BASE_URL (str): Template base da URL usada para compor os links de scraping.

    Mapeamentos usados:

        Vocações:
            "None": 1
            "Knight": 2
            "Paladin": 3
            "Sorcerer": 4
            "Druid": 5
            "Monk": 6

        Battle Eye:
            "Todos": -1
            "Unprotected": 0
            "Protected": 1
            "Initially Protected": 2

        Tipos de Mundo:
            "Open PvP": 0
            "Optional PvP": 1
            "Hardcore PvP": 2
            "Retro Open PvP": 3
            "Retro Hardcore PvP": 4

        Categoria base:
            "Experience Points": 6
    """

    def __init__(self):
        """
        Inicializa o scraper de highscores por vocação.
        """
        logger.info("Inicializando Vocation...")
        self.table = HighScore()
        self.BASE_URL = (
            "https://www.tibia.com/community/?subtopic=highscores&world="
            "&beprotection=-1"
            "&worldtypes[]={world_type}"
            "&category=6"
            "&profession={vocation_id}"
            "&currentpage={page_number}"
        )
        logger.info("HighscoreVocation inicializado com sucesso.")

    def processar_paginas(self, lista_paginas):
        """
        Itera sobre uma lista de URLs e concatena os resultados de todas as páginas em um único DataFrame.

        Args:
            lista_paginas (list): Lista contendo as URLs das páginas de highscores.

        Returns:
            pandas.DataFrame: DataFrame consolidado com os resultados de todas as páginas.
        """
        logger.info(f"Iniciando processamento de {len(lista_paginas)} páginas.")
        dataframe = pd.DataFrame()

        for pagina in lista_paginas:
            logger.debug(f"Processando página: {pagina}")
            result = self.table.scraper(pagina)
            if isinstance(result, pd.DataFrame) and not result.empty:
                logger.debug(f"{len(result)} linhas extraídas.")
                dataframe = pd.concat([dataframe, result], ignore_index=True)
            else:
                logger.warning(f"Nenhum dado encontrado em: {pagina}")

            # Intervalo aleatório entre 1 e 3 segundos
            time.sleep(random.uniform(1, 3))

        logger.info(f"Total de linhas processadas: {len(dataframe)}")
        return dataframe

    def get_vocation_data(self, vocation_id):
        """
        Coleta os dados de uma vocação específica (como Knight, Paladin, etc.)
        em todos os tipos de mundo do Tibia.

        Args:
            vocation_id (int): Código numérico da vocação (0 a 6).

        Returns:
            pandas.DataFrame: DataFrame com os dados de todas as combinações de tipo de mundo
            e páginas da vocação especificada. Adiciona a coluna 'WorldType' indicando o tipo de servidor.
        """
        logger.info(f"Iniciando coleta para vocação ID: {vocation_id}")

        world_types = {
            "Open PvP": [
                self.BASE_URL.format(world_type=0, vocation_id=vocation_id, page_number=i)
                for i in range(1, 21)
            ],
            "Optional PvP": [
                self.BASE_URL.format(world_type=1, vocation_id=vocation_id, page_number=i)
                for i in range(1, 21)
            ],
            "Hardcore PvP": [
                self.BASE_URL.format(world_type=2, vocation_id=vocation_id, page_number=i)
                for i in range(1, 21)
            ],
            "Retro Open PvP": [
                self.BASE_URL.format(world_type=3, vocation_id=vocation_id, page_number=i)
                for i in range(1, 21)
            ],
            "Retro Hardcore PvP": [
                self.BASE_URL.format(world_type=4, vocation_id=vocation_id, page_number=i)
                for i in range(1, 21)
            ],
        }

        todos_os_dados = []

        for world_name, lista_paginas in world_types.items():
            logger.info(f"Coletando dados para mundo: {world_name}")
            df = self.processar_paginas(lista_paginas)
            if not df.empty:
                logger.info(f"{len(df)} entradas coletadas de {world_name}.")
                df["WorldType"] = world_name
                todos_os_dados.append(df)
            else:
                logger.warning(f"Nenhum dado encontrado para {world_name}.")

        if todos_os_dados:
            resultado = pd.concat(todos_os_dados, ignore_index=True)
            logger.info(f"Coleta concluída. Total de linhas: {len(resultado)}")
            return resultado
        else:
            logger.warning("Coleta finalizou sem dados.")
            return pd.DataFrame()

    # Métodos específicos para cada vocação
    def no_vocation(self):
        """Retorna o ranking geral de personagens sem vocação."""
        logger.info("Obtendo ranking: No Vocation")
        return self.get_vocation_data(1)

    def knight(self):
        """Retorna o ranking dos Knights."""
        logger.info("Obtendo ranking: Knight")
        return self.get_vocation_data(2)

    def paladin(self):
        """Retorna o ranking dos Paladins."""
        logger.info("Obtendo ranking: Paladin")
        return self.get_vocation_data(3)

    def sorcerer(self):
        """Retorna o ranking dos Sorcerers."""
        logger.info("Obtendo ranking: Sorcerer")
        return self.get_vocation_data(4)

    def druid(self):
        """Retorna o ranking dos Druids."""
        logger.info("Obtendo ranking: Druid")
        return self.get_vocation_data(5)

    def monk(self):
        """Retorna o ranking dos Monks."""
        logger.info("Obtendo ranking: Monk")
        return self.get_vocation_data(6)


class Category:
    """
    Classe responsável por coletar dados dos rankings (highscores) de habilidades no site oficial do Tibia.

    Essa classe realiza o scraping das páginas de highscores por categoria (como sword fighting, magic level, etc.)
    e por vocação (como Knight, Paladin, etc.)

    A coleta é feita de forma automatizada, iterando pelas páginas dos rankings e retornando os dados em um DataFrame do pandas.

    Attributes:
        table (HighScore): Instância da classe HighScore utilizada para processar o scraping de cada página.
        BASE_URL (str): Template da URL base usada para compor as páginas de highscores.
        categories (dict): Dicionário que mapeia as categorias de habilidade com seus respectivos IDs,
            nomes legíveis e a vocação associada (ou 0 para todas as vocações).

            Exemplo:
            {
                "sword": (13, "Sword Fighting", 2),
                "fist": (8, "Fist Fighting", 0)
            }
    """

    def __init__(self):
        """
        Inicializa a classe Skills com um driver Selenium e configura os parâmetros base de scraping.
        """
        logger.info("Inicializando Skills...")

        self.table = HighScore()
        self.BASE_URL = (
            "https://www.tibia.com/community/?subtopic=highscores&world="
            "&beprotection=-1"
            "&category={category_id}"
            "&profession={vocation_id}"
            "&currentpage={page_number}"
        )

        self.categories = {
            "achievements": (1, "Achievements", 0),
            "axe": (2, "Axe Fighting", 2),
            "sword": (13, "Sword Fighting", 2),
            "club": (4, "Club Fighting", 2),
            "distance": (5, "Distance Fighting", 3),
            "magic": (11, "Magic Level", 4),
            "fist": (8, "Fist Fighting", 0),
            "shielding": (12, "Shielding", 0),
            "fishing": (7, "Fishing", 0),
            "loyalty": (10, "Loyalty Points", 0),
            "drome": (14, "Drome Score", 0),
            "boss": (15, "Boss Points", 0),
            "charm": (3, "Charm Points", 0),
            "goshnair": (9, "Goshnar's Taint", 0),
        }

        logger.info("HighscoreSkills inicializado com sucesso.")

    def processar_paginas(self, lista_paginas):
        """
        Itera sobre uma lista de URLs e concatena os dados extraídos de cada página em um único DataFrame.

        Args:
            lista_paginas (list): Lista de URLs de páginas de highscores.

        Returns:
            pandas.DataFrame: DataFrame contendo todos os dados combinados.
        """
        logger.info(f"Processando {len(lista_paginas)} páginas de highscores.")
        dataframe = pd.DataFrame()

        for pagina in lista_paginas:
            logger.debug(f"Scraping da página: {pagina}")
            result = self.table.scraper(pagina)
            if isinstance(result, pd.DataFrame) and not result.empty:
                logger.debug(f"{len(result)} linhas coletadas da página.")
                dataframe = pd.concat([dataframe, result], ignore_index=True)
            else:
                logger.warning(f"Nenhum dado encontrado na página: {pagina}")
            # Intervalo aleatório entre 1 e 3 segundos
            time.sleep(random.uniform(1, 3))

        logger.info(f"Total de linhas coletadas: {len(dataframe)}")
        return dataframe

    def get_data(self, category_id, category_name, vocation_id):
        """
        Coleta os dados de uma determinada categoria e vocação a partir das páginas do site.

        Gera as URLs dinamicamente de acordo com a categoria e vocação especificadas,
        e retorna um DataFrame consolidado com os resultados.

        Args:
            category_id (int): Código da categoria (ex: 1 = Achievements, 2 = Axe, etc.).
            category_name (str): Nome da categoria (ex: "Axe Fighting").
            vocation_id (int): ID da vocação (ex: 2 = Knight, 0 = Todas).

        Returns:
            pandas.DataFrame: DataFrame com os dados da categoria especificada. Contém também
            as colunas 'category' e 'vocation_id' para identificar o tipo de dado.
        """
        logger.info(f"Coletando dados para categoria '{category_name}' (ID: {category_id}) | Vocação ID: {vocation_id}")
        
        lista_paginas = [
            self.BASE_URL.format(
                category_id=category_id,
                vocation_id=vocation_id,
                page_number=i
            ) for i in range(1, 21)
        ]

        df = self.processar_paginas(lista_paginas)
        if not df.empty:
            logger.info(f"Coleta finalizada para '{category_name}'. Total: {len(df)} linhas.")
            df["category"] = category_name
            df["vocation_id"] = vocation_id
            return df
        else:
            logger.warning(f"Nenhum dado retornado para '{category_name}' (ID: {category_id})")
            return pd.DataFrame()

    def get_by_category(self, category):
        """
        Busca os dados de ranking de uma categoria pré-definida.

        Args:
            category (str): Nome da categoria (deve estar presente no dicionário `self.categories`).

        Returns:
            pandas.DataFrame: DataFrame contendo os dados de ranking da categoria escolhida.

        Raises:
            ValueError: Se a categoria fornecida não for válida.
        """
        logger.info(f"Solicitado ranking da categoria: {category}")
        if category not in self.categories:
            logger.error(f"Categoria inválida: {category}")
            raise ValueError("Categoria inválida.")
        
        category_id, category_name, vocation_id = self.categories[category]
        return self.get_data(category_id, category_name, vocation_id)
