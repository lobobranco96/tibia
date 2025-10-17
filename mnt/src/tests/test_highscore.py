import pandas as pd
import os
from bronze.extract import HighScore

# Caminho do arquivo HTML local
HTML_FILE = os.path.join(os.path.dirname(__file__), "html/highscore_page.html")

def test_scraper_mock_from_file(monkeypatch):
    """
    Testa o scraper usando HTML local lido de arquivo, sem fazer requests reais.
    """
    highscore = HighScore()

    # Lê o HTML do arquivo
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        html_content = f.read()

    # Mock do requests.get para retornar o HTML do arquivo
    class MockResponse:
        def __init__(self, text):
            self.content = text.encode("utf-8")

    def mock_get(url, headers=None, timeout=None):
        return MockResponse(html_content)

    # Patch requests.get
    monkeypatch.setattr("bronze.extract.requests.get", mock_get)

    # Chama o scraper (URL qualquer, será ignorado pelo mock)
    df = highscore.scraper("http://fakeurl.com")

    # Verifica se é DataFrame
    assert isinstance(df, pd.DataFrame)

    # Verifica colunas esperadas
    expected_columns = ["Rank", "Name", "Vocation", "World", "Level", "Points"]
    missing_cols = [c for c in expected_columns if c not in df.columns]
    assert missing_cols == [], f"Faltando colunas: {missing_cols}"

    # Verifica que a coluna 'Rank' não possui valores nulos
    assert df["Rank"].notnull().all(), "Coluna 'Rank' contém valores nulos"

    # Verifica o número de linhas, cada pagina deve retornar 50 linhas / 50 ranks
    assert len(df) == 50, f"Número de linhas incorreto: {len(df)}"
