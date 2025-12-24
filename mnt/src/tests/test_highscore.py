import pandas as pd
import os
from landing.scraper import BuscadorPagina, HighScoreParser

# Caminho do arquivo HTML local
HTML_FILE = os.path.join(os.path.dirname(__file__), "html/highscore_page.html")

def test_scraper_mock_from_file(monkeypatch):
    """
    Testa o parser de highscores usando HTML local (sem requests reais).
    """
    fetcher = BuscadorPagina()
    parser = HighScoreParser()

    # Lê o HTML do arquivo
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        html_content = f.read()

    # Mock do requests.get para retornar o HTML do arquivo
    class MockResponse:
        def __init__(self, text):
            self.content = text.encode("utf-8")

        def raise_for_status(self):
            # Simula comportamento bem-sucedido
            pass

    def mock_get(url, headers=None, timeout=None):
        return MockResponse(html_content)

    # Patch requests.get
    monkeypatch.setattr("landing.extract.requests.get", mock_get)

    # Chama o fetcher normalmente (sem requests reais)
    html = fetcher.get("http://fakeurl.com")

    # Parseia o HTML
    df = parser.parse(html)

    # Testes de estrutura
    assert isinstance(df, pd.DataFrame)
    expected_columns = ["Rank", "Name", "Vocation", "World", "Level", "Points"]
    missing_cols = [c for c in expected_columns if c not in df.columns]
    assert missing_cols == [], f"Faltando colunas: {missing_cols}"

    # Testes de conteúdo
    assert df["Rank"].notnull().all(), "Coluna 'Rank' contém valores nulos"

    assert len(df) == 50, f"Número de linhas incorreto: {len(df)}"
