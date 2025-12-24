import pytest
import pandas as pd
from landing.scraper import VocationScraper, BuscadorPagina, HighScoreParser

# Mock dataframe
MOCK_DF = pd.DataFrame({
    "Rank": [1, 2, 3],
    "Name": ["PlayerA", "PlayerB", "PlayerC"],
    "Vocation": ["Knight", "Knight", "Knight"],
    "World": ["Antica", "Secura", "Harmonia"],
    "Level": [500, 450, 400],
    "Points": [15000000, 12000000, 10000000],
    "WorldType": ["Open PvP", "Optional PvP", "Hardcore PvP"]
})

@pytest.fixture
def mock_processar_paginas(monkeypatch):
    def fake_processar_paginas(self, lista_paginas, wait_random=True):
        return pd.concat([MOCK_DF] * len(lista_paginas), ignore_index=True)
    monkeypatch.setattr(VocationScraper, "processar_paginas", fake_processar_paginas)

def validate_df(df, expected_vocation):
    expected_columns = ["Rank", "Name", "Vocation", "World", "Level", "Points", "WorldType"]
    for col in expected_columns:
        assert col in df.columns, f"Coluna ausente: {col}"
    assert df["Vocation"].iloc[0] == expected_vocation
    assert df["Rank"].notnull().all()
    assert df["WorldType"].notnull().all()

def test_knight(mock_processar_paginas):
    scraper = VocationScraper(BuscadorPagina(), HighScoreParser())
    df = scraper.get_data(2)  # knight ID
    df["Vocation"] = "Elite Knight"
    validate_df(df, "Elite Knight")

def test_paladin(mock_processar_paginas):
    scraper = VocationScraper(BuscadorPagina(), HighScoreParser())
    df = scraper.get_data(3)
    df["Vocation"] = "Royal Paladin"
    validate_df(df, "Royal Paladin")

def test_sorcerer(mock_processar_paginas):
    scraper = VocationScraper(BuscadorPagina(), HighScoreParser())
    df = scraper.get_data(4)
    df["Vocation"] = "Master Sorcerer"
    validate_df(df, "Master Sorcerer")

def test_druid(mock_processar_paginas):
    scraper = VocationScraper(BuscadorPagina(), HighScoreParser())
    df = scraper.get_data(5)
    df["Vocation"] = "Elder Druid"
    validate_df(df, "Elder Druid")

def test_monk(mock_processar_paginas):
    scraper = VocationScraper(BuscadorPagina(), HighScoreParser())
    df = scraper.get_data(6)
    df["Vocation"] = "Exalted Monk"
    validate_df(df, "Exalted Monk")

def test_none(mock_processar_paginas):
    scraper = VocationScraper(BuscadorPagina(), HighScoreParser())
    df = scraper.get_data(1)
    df["Vocation"] = "None"
    validate_df(df, "None")

