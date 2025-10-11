from utility import selenium_webdriver
import pandas as pd
from bs4 import BeautifulSoup

class HighScore:
  def __init__(self, selenium_webdriver):
    self.selenium_driver = selenium_webdriver

  def scraper(self, url):
    self.selenium_driver.get(url)
    soup = BeautifulSoup(self.selenium_driver.page_source, "lxml")
    rows = soup.find("table", class_="Table3").find_all('tr')
    if len(rows) < 3:
        return pd.DataFrame()

    content = rows[2]
    inner_table = content.find("table", class_="TableContent")
    if not inner_table:
        return pd.DataFrame()

    table = inner_table.find_all('tr')
    if not table:
        return pd.DataFrame()

    columns = [col.get_text(strip=True) for col in table[0].find_all(['td', 'th'])]
    data = []
    for row in table[1:]:
        values = [r.get_text(strip=True) for r in row.find_all(['td', 'th'])]
        data.append(values)

    return pd.DataFrame(data, columns=columns)

class HighscoreVocation:
    """
    Classe para encapsular o scraping de diferentes vocações do Tibia.
    Cada vocação tem uma função que retorna um DataFrame específico.

    Instruções:

    Vocações
    "None":1,
    "Knight":2,
    "Paladin":3,
    "Sorcerer":4,
    "Druid":5,
    "Monk":6

    Battle Eye
    "Todos": -1,
    "Unprotected": 0,
    "Protected": 1,
    "Initially Protected": 2

    World Types:
    "Open PvP": 0,
    "Optional PvP": 1,
    "Hardcore PvP": 2, 
    "Retro Open PvP": 3, 
    "Retro Hardcore PvP": 4

    Categoria base
    "Experience Points": 6,

    Total de paginas = 20
    Return total 1000 characters

    Colunas esperadas:
    Rank: Rank atual
    Name: Nome do player
    Vocation: Vocação do character
    World: Mundo
    Level: Nivel do character
    Points: Quantidade total em experiencia

    """
    def __init__(self):
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
        dataframe = pd.DataFrame()

        for pagina in lista_paginas:
            result = self.table.scraper(pagina)
            if isinstance(result, pd.DataFrame) and not result.empty:
                dataframe = pd.concat([dataframe, result], ignore_index=True)

        return dataframe

    def get_vocation_data(self, vocation_id):

      # Define os tipos de mundos e suas URLs
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
              df["WorldType"] = world_name  # adiciona a coluna com o nome do tipo de mundo
              todos_os_dados.append(df)

      # Concatena todos os DataFrames em um único
      if todos_os_dados:
          return pd.concat(todos_os_dados, ignore_index=True)
      else:
          return pd.DataFrame()  # Retorna vazio se nenhum dado foi encontrado

    # Métodos específicos para cada vocação
    def no_vocation(self):
        return self.get_vocation_data(1)

    def knight(self):
        return self.get_vocation_data(2)

    def paladin(self):
        return self.get_vocation_data(3)

    def sorcerer(self):
        return self.get_vocation_data(4)

    def druid(self):
        return self.get_vocation_data(5)

    def monk(self):
        return self.get_vocation_data(6)