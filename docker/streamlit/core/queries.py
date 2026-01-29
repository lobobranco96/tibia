from core.duckdb_session import get_duckdb_connection
from core.config import LAKEHOUSE

def experience_global_rank():
    con = get_duckdb_connection()

    query = f"""
        SELECT
            rank,
            name,
            world,
            vocation,
            level,
            experience,
            world_type,
            updated_at
        FROM read_parquet(
            's3://{LAKEHOUSE["bucket"]}/{LAKEHOUSE["gold_path"]}/experience_rank_global_7e91a343-db2b-45f1-b507-d047fd991d1d/data/*'
        )
    """

    return con.execute(query).df()
