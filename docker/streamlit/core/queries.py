import pandas as pd
from core.trino_session import get_trino_connection

# EXPERIENCE GLOBAL RANK
def experience_global_rank(snapshot_date=None):
    conn = get_trino_connection()

    if snapshot_date:
        snapshot_date = str(snapshot_date)[:10]

        query = f"""
            SELECT DISTINCT
                rank,
                name,
                world,
                vocation,
                level,
                experience,
                world_type,
                updated_at,
                snapshot_date
            FROM nessie.gold.experience_global_rank
            WHERE snapshot_date = DATE '{snapshot_date}'
            ORDER BY rank
        """
    else:
        query = """
            SELECT DISTINCT
                rank,
                name,
                world,
                vocation,
                level,
                experience,
                world_type,
                updated_at,
                snapshot_date
            FROM nessie.gold.experience_global_rank
            ORDER BY snapshot_date DESC, rank
        """

    return pd.read_sql(query, conn)

# SKILLS GLOBAL RANK
def skills_global_rank(snapshot_date=None):
    conn = get_trino_connection()

    if snapshot_date:
        query = f"""
            SELECT
                rank,
                name,
                world,
                skill_name,
                vocation,
                skill_level,
                updated_at,
                snapshot_date
            FROM nessie.gold.skills_global_rank
            WHERE snapshot_date = DATE '{snapshot_date}'
            ORDER BY skill_name, rank
        """
    else:
        query = """
            SELECT
                rank,
                name,
                world,
                skill_name,
                vocation,
                skill_level,
                updated_at,
                snapshot_date
            FROM nessie.gold.skills_global_rank
        """

    return pd.read_sql(query, conn)


# WORLD SUMMARY
def world_summary():
    conn = get_trino_connection()

    query = """
        SELECT
            world,
            world_type,
            vocation,
            players_count,
            updated_at
        FROM nessie.gold.world_summary
    """

    return pd.read_sql(query, conn)

# PLAYER PROGRESSION
def player_progression():
    conn = get_trino_connection()

    query = """
        SELECT
            name,
            world,
            vocation,
            world_type,
            previous_level,
            current_level,
            level_gain,
            previous_experience,
            current_experience,
            experience_gain,
            previous_start_date,
            current_start_date,
            days_between_updates,
            avg_xp_per_day,
            is_current
        FROM nessie.gold.player_progression
    """

    return pd.read_sql(query, conn)

# SKILL PROGRESSION
def skill_progression():
    conn = get_trino_connection()

    query = """
        SELECT
            name,
            world,
            vocation,
            category,
            skill_before,
            skill_after,
            skill_gain,
            from_date,
            to_date,
            days_between_updates,
            avg_skill_per_day,
            is_current
        FROM nessie.gold.skills_progression
    """

    return pd.read_sql(query, conn)

