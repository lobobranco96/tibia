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
            updated_at,
            snapshot_date
        FROM read_parquet(
            's3://{LAKEHOUSE["bucket"]}/{LAKEHOUSE["gold_path"]}/experience_rank_global_7e91a343-db2b-45f1-b507-d047fd991d1d/data/*'
        )
    """

    return con.execute(query).df()

def skills_global_rank():
    con = get_duckdb_connection()

    query = f"""
        SELECT
            name
            , world
            , skill_name
            , vocation
            , skill_level
            , updated_at
        FROM read_parquet(
            's3://{LAKEHOUSE["bucket"]}/{LAKEHOUSE["gold_path"]}/skills_rank_global_15cd9380-9d65-4dc0-8313-0caa64ab6e24/data/*'
        )
    """
    
    return con.execute(query).df()

def world_summary():
    con = get_duckdb_connection()

    query = f"""
        SELECT
	        world
            , world_type
            , vocation
            , players_count
            , updated_at
        FROM read_parquet(
            's3://{LAKEHOUSE["bucket"]}/{LAKEHOUSE["gold_path"]}/world_summary_f0651177-c57d-432d-bf9b-44db00a3e865/data/*'
        )
    """

    return con.execute(query).df()
