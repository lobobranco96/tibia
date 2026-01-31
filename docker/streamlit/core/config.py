# core/config.py
import os

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")

MINIO_CONFIG = {
    "endpoint": S3_ENDPOINT.replace("http://", ""),
    "access_key": AWS_ACCESS_KEY,
    "secret_key": AWS_SECRET_KEY,
    "region": "us-east-1",
    "use_ssl": False
}
DUCKDB = {
    "database": ":memory:"
}

LAKEHOUSE = {
    "bucket": "lakehouse",
    "gold_path": "gold"
}
