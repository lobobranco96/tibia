import pandas as pd
from core.trino_session import get_trino_connection

# EXPERIENCE GLOBAL RANK
def experience_global_rank(snapshot_date=None):
    conn = get_trino_connection()

    base_query = """
   WITH dedup AS (
        SELECT
            name,
            world,
            vocation,
            level,
            experience,
            world_type,
            updated_at,
            snapshot_date,
            ROW_NUMBER() OVER (
                PARTITION BY snapshot_date, name
                ORDER BY updated_at DESC
            ) AS rn
        FROM nessie.gold.experience_global_rank
    ),
    latest AS (
        SELECT *
        FROM dedup
        WHERE rn = 1
    )
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY snapshot_date
            ORDER BY experience DESC, level DESC, name ASC
        ) AS rank,
        name,
        world,
        vocation,
        level,
        experience,
        world_type,
        updated_at,
        snapshot_date
    FROM latest
    """

    if snapshot_date:
        snapshot_date = str(snapshot_date)[:10]
        base_query += f" WHERE snapshot_date = DATE '{snapshot_date}'"

    base_query += " ORDER BY snapshot_date DESC, rank"

    return pd.read_sql(base_query, conn)


# SKILLS GLOBAL RANK
def skills_global_rank(snapshot_date=None):
    conn = get_trino_connection()

    base_query = """
    WITH dedup AS (
        SELECT
            name,
            world,
            skill_name,
            vocation,
            skill_level,
            updated_at,
            snapshot_date,
            ROW_NUMBER() OVER (
                PARTITION BY snapshot_date, name, skill_name
                ORDER BY updated_at DESC
            ) AS rn
        FROM nessie.gold.skills_global_rank
    ),
    latest AS (
        SELECT *
        FROM dedup
        WHERE rn = 1
    )
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY snapshot_date, skill_name
            ORDER BY skill_level DESC, name ASC
        ) AS rank,
        name,
        world,
        skill_name,
        vocation,
        skill_level,
        updated_at,
        snapshot_date
    FROM latest
    """

    if snapshot_date:
        snapshot_date = str(snapshot_date)[:10]
        base_query += f" WHERE snapshot_date = DATE '{snapshot_date}'"

    base_query += " ORDER BY snapshot_date DESC, skill_name, rank"

    return pd.read_sql(base_query, conn)


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


