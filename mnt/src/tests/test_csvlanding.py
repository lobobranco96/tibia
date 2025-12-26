from unittest.mock import MagicMock
from landing.utility import CSVLanding
import pandas as pd
import io

def test_write_creates_object_in_s3():
    df = pd.DataFrame({"Nome": ["Renato", "Mariano"], "Idade": [28, 29]})

    landing = CSVLanding()
    landing.s3 = MagicMock()
    landing.bucket_name = "test-bucket"

    result = landing.write(df, "teste", "arquivo")

    landing.s3.put_object.assert_called_once()
    assert result["rows"] == len(df)


def test_write_sends_correct_csv_content():
    df = pd.DataFrame({"Nome": ["Renato"], "Idade": [28]})

    landing = CSVLanding()
    landing.s3 = MagicMock()
    landing.bucket_name = "test-bucket"

    landing.write(df, "pessoas", "dados")

    # Captura o CSV enviado ao S3
    args, kwargs = landing.s3.put_object.call_args
    csv_body = kwargs["Body"]

    reloaded = pd.read_csv(io.StringIO(csv_body))
    pd.testing.assert_frame_equal(df, reloaded)


def test_write_metadata():
    df = pd.DataFrame({"Nome": ["Renato"]})

    landing = CSVLanding()
    landing.s3 = MagicMock()
    landing.bucket_name = "test-bucket"

    result = landing.write(df, "teste", "dados")

    assert set(result.keys()) == {"bucket", "key", "rows", "columns", "timestamp"}
