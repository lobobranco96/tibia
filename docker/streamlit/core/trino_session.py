import trino

def get_trino_connection():
    return trino.dbapi.connect(
        host="localhost",
        port=8080,
        user="streamlit",
        catalog="iceberg",
        schema="gold"
    )
