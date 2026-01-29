import duckdb
from core.config import MINIO_CONFIG, DUCKDB

def get_duckdb_connection():
    con = duckdb.connect(DUCKDB["database"])

    # extensões
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    # MinIO (S3 compatível)
    con.execute(f"""
        SET s3_endpoint='{MINIO_CONFIG["endpoint"]}';
        SET s3_access_key_id='{MINIO_CONFIG["access_key"]}';
        SET s3_secret_access_key='{MINIO_CONFIG["secret_key"]}';
        SET s3_region='{MINIO_CONFIG["region"]}';
        SET s3_url_style='path';
        SET s3_use_ssl=false;
    """)

    return con
