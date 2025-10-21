import os
from landing.utility import CSVLanding
import pandas as pd

def test_write_creates_file(tmp_path):
    data = {"Nome": ["Renato", "Mariano"], "Idade": [28, 29]}
    df = pd.DataFrame(data)

    landing = CSVLanding()
    landing.base_dir = tmp_path  

    result = landing.write(df, "teste", "arquivo")

    assert os.path.exists(result["path"])
    assert result["rows"] == len(df)

def test_write_saves_correct_data(tmp_path):
    df = pd.DataFrame({"Nome": ["Renato"], "Idade": [28]})
    landing = CSVLanding()
    landing.base_dir = tmp_path

    result = landing.write(df, "pessoas", "dados")
    reloaded = pd.read_csv(result["path"])

    pd.testing.assert_frame_equal(df, reloaded)


def test_write_metadata(tmp_path):
    df = pd.DataFrame({"Nome": ["Renato"]})
    landing = CSVLanding()
    landing.base_dir = tmp_path

    result = landing.write(df, "teste", "dados")

    assert set(result.keys()) == {"path", "rows", "columns", "timestamp"}