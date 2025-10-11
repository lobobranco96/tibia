from utility import selenium_webdriver
import pandas as pd
from bs4 import BeautifulSoup


class HighScore:
    """
    Classe responsável por realizar o scraping de dados de uma tabela de Highscores
    no site oficial do Tibia. Utiliza Selenium para carregar a página e BeautifulSoup
    para extrair o conteúdo da tabela.

    Attributes:
        selenium_driver: Instância do webdriver Selenium configurada para navegação automatizada.
    """

    def __init__(self, selenium_webdriver):
        """
        Inicializa a classe HighScore com um driver Selenium ativo.

        Args:
            selenium_webdriver: Instância do webdriver (ex: Chrome, Firefox)
                criada externamente para manipulação de páginas web.
        """
        self.selenium_driver = selenium_webdriver

    def scraper(self, url):
        """
        Realiza o scraping de uma única página de highscores e retorna os dados em formato DataFrame.

        O método carrega a página fornecida, localiza a tabela principal contendo as
        informações de ranking e transforma os dados extraídos em um DataFrame do pandas.

        Args:
            url (str): URL da página de highscores a ser processada.

        Returns:
            pandas.DataFrame: DataFrame contendo as colunas extraídas da tabela.
            Caso a página não possua dados válidos, retorna um DataFrame vazio.
        """
        self.selenium_driver.get(url)
        soup = BeautifulSoup(self.selenium_driver.page_source, "lxml")

        table_container = soup.find("table", class_="Table3")
        if not table_container:
            return pd.DataFrame()

        rows = table_container.find_all('tr')
        if len(rows) < 3:
            return pd.DataFrame()

        content = rows[2]
        inner_table = content.find("table", class_="TableContent")
        if not inner_table:
            return pd.DataFrame()

        table = inner_table.find_all('tr')
        if not table:
            return pd.DataFrame()

        # Extrai cabeçalhos e conteúdo da tabela
        columns = [col.get_text(strip=True) for col in table[0].find_all(['td', 'th'])]
        data = []
        for row in table[1:]:
            values = [r.get_text(strip=True) for r in row.find_all(['td', 'th'])]
            data.append(values)

        return pd.DataFrame(data, columns=columns)


class HighscoreVocation:
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
        Inicializa a classe HighscoreVocation com o driver Selenium e o template da URL base.
        """
        selenium_driver = selenium_webdriver()
        self.table = HighScore(selenium_driver)
        self.BASE_URL = (
            "https://www.tibia.com/community/?subtopic=highscores&world="
            "&beprotection=-1"
            "&worldtypes[]={world_type}"
            "&category=6"
            "&profession={vocation}"
            "&currentpage={page_number}"
        )

    def processar_paginas(self, lista_paginas):
        """
        Itera sobre uma lista de URLs e concatena os resultados de todas as páginas em um único DataFrame.

        Args:
            lista_paginas (list): Lista contendo as URLs das páginas de highscores.

        Returns:
            pandas.DataFrame: DataFrame consolidado com os resultados de todas as páginas.
        """
        dataframe = pd.DataFrame()

        for pagina in lista_paginas:
            result = self.table.scraper(pagina)
            if isinstance(result, pd.DataFrame) and not result.empty:
                dataframe = pd.concat([dataframe, result], ignore_index=True)

        return dataframe

    def get_vocation_data(self, vocation_id):
        """
        Coleta os dados de uma vocação específica (como Knight, Paladin, etc.)
        em todos os tipos de mundo do Tibia.

        Args:
            vocation_id (int): Código numérico da vocação (1 a 6).

        Returns:
            pandas.DataFrame: DataFrame com os dados de todas as combinações de tipo de mundo
            e páginas da vocação especificada. Adiciona a coluna 'WorldType' indicando o tipo de servidor.
        """
        world_types = {
            "Open PvP": [
                self.BASE_URL.format(world_type=0, vocation=vocation_id, page_number=i)
                for i in range(1, 21)
            ],
            "Optional PvP": [
                self.BASE_URL.format(world_type=1, vocation=vocation_id, page_number=i)
                for i in range(1, 21)
            ],
            "Hardcore PvP": [
                self.BASE_URL.format(world_type=2, vocation=vocation_id, page_number=i)
                for i in range(1, 21)
            ],
            "Retro Open PvP": [
                self.BASE_URL.format(world_type=3, vocation=vocation_id, page_number=i)
                for i in range(1, 21)
            ],
            "Retro Hardcore PvP": [
                self.BASE_URL.format(world_type=4, vocation=vocation_id, page_number=i)
                for i in range(1, 21)
            ],
        }

        todos_os_dados = []

        for world_name, lista_paginas in world_types.items():
            df = self.processar_paginas(lista_paginas)
            if not df.empty:
                df["WorldType"] = world_name  # adiciona a coluna com o tipo de mundo
                todos_os_dados.append(df)

        # Concatena todos os DataFrames
        if todos_os_dados:
            return pd.concat(todos_os_dados, ignore_index=True)
        else:
            return pd.DataFrame()

    # Métodos específicos para cada vocação
    def no_vocation(self):
        """Retorna o ranking geral de personagens sem vocação."""
        return self.get_vocation_data(1)

    def knight(self):
        """Retorna o ranking dos Knights."""
        return self.get_vocation_data(2)

    def paladin(self):
        """Retorna o ranking dos Paladins."""
        return self.get_vocation_data(3)

    def sorcerer(self):
        """Retorna o ranking dos Sorcerers."""
        return self.get_vocation_data(4)

    def druid(self):
        """Retorna o ranking dos Druids."""
        return self.get_vocation_data(5)

    def monk(self):
        """Retorna o ranking dos Monks."""
        return self.get_vocation_data(6)
