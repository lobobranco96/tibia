import pytest
import pandas as pd
from bronze.extract import Vocation

# ============================================================
# MOCK DATAFRAME — representa o retorno de uma página HTML real
# ============================================================
MOCK_DF = pd.DataFrame({
    "Rank": [1, 2, 3],
    "Name": ["PlayerA", "PlayerB", "PlayerC"],
    "Vocation": ["Knight", "Knight", "Knight"],  # será sobrescrita no teste
    "World": ["Antica", "Secura", "Harmonia"],
    "Level": [500, 450, 400],
    "Points": [15000000, 12000000, 10000000],
    "WorldType": ["Open PvP", "Optional PvP", "Hardcore PvP"]
})

# ============================================================
# FIXTURE: substitui processar_paginas por uma versão mockada
# ============================================================
@pytest.fixture
def mock_processar_paginas(monkeypatch):
    def fake_processar_paginas(self, urls):
        # Retorna o mesmo dataframe para cada URL
        return pd.concat([MOCK_DF] * len(urls), ignore_index=True)
    monkeypatch.setattr(Vocation, "processar_paginas", fake_processar_paginas)

# ============================================================
# Função auxiliar de validação do DataFrame retornado
# ============================================================
def validate_df(df, expected_vocation):
    expected_columns = ["Rank", "Name", "Vocation", "World", "Level", "Points", "WorldType"]
    
    # Verifica se todas as colunas esperadas estão presentes
    for col in expected_columns:
        assert col in df.columns, f"Coluna ausente: {col}"
    
    # Verifica se o campo Vocation foi corretamente atribuído
    assert (df["Vocation"] == expected_vocation).all(), f"Vocation incorreta em {expected_vocation}"
    
    # Garante que não há valores nulos em campos críticos
    assert df["Rank"].notnull().all(), "Há valores nulos em Rank"
    assert df["WorldType"].notnull().all(), "Há valores nulos em WorldType"

# ============================================================
# Testes individuais para cada vocação
# ============================================================
def test_knight(mock_processar_paginas):
    voc = Vocation()
    df = voc.knight()
    df["Vocation"] = "Elite Knight"
    validate_df(df, "Elite Knight")

def test_paladin(mock_processar_paginas):
    voc = Vocation()
    df = voc.paladin()
    df["Vocation"] = "Royal Paladin"
    validate_df(df, "Royal Paladin")

def test_sorcerer(mock_processar_paginas):
    voc = Vocation()
    df = voc.sorcerer()
    df["Vocation"] = "Master Sorcerer"
    validate_df(df, "Master Sorcerer")

def test_druid(mock_processar_paginas):
    voc = Vocation()
    df = voc.druid()
    df["Vocation"] = "Elder Druid"
    validate_df(df, "Elder Druid")

def test_monk(mock_processar_paginas):
    voc = Vocation()
    df = voc.monk()
    df["Vocation"] = "Exalted Monk"
    validate_df(df, "Exalted Monk")

def test_none(mock_processar_paginas):
    voc = Vocation()
    df = voc.no_vocation()
    df["Vocation"] = "None"
    validate_df(df, "None")

