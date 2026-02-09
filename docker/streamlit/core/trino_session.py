from trino.dbapi import connect

def get_trino_connection():
    conn = connect(
        host="trino",
        port=8080,
        user="streamlit",
        catalog="iceberg",
        schema="gold"
    )
    return conn
