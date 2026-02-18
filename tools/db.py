"""
db.py — Database connection and helper functions.

Provides connection pooling and common query helpers for the
Harvest Med Waste lead database (PostgreSQL).

Usage:
    from tools.db import get_conn, execute, fetch_all, fetch_one
"""

import os
import json
import psycopg2
import psycopg2.extras
from contextlib import contextmanager

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
except ImportError:
    pass

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://harvest:harvest_dev@localhost:5432/harvest_leads"
)

_conn = None


def get_conn():
    """Get or create a persistent database connection."""
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(DATABASE_URL)
        _conn.autocommit = False
    return _conn


@contextmanager
def get_cursor(commit=True):
    """Context manager for a database cursor. Commits on success, rolls back on error."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield cur
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def execute(sql, params=None):
    """Execute a single SQL statement."""
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount


def fetch_all(sql, params=None):
    """Execute SQL and return all rows as list of dicts."""
    with get_cursor(commit=False) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def fetch_one(sql, params=None):
    """Execute SQL and return a single row as dict (or None)."""
    with get_cursor(commit=False) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None


def upsert_lead(lead_data):
    """Insert or update a lead record. Returns the lead id."""
    fields = [
        "lead_uid", "facility_name", "facility_type", "address_line1",
        "address_line2", "city", "state", "zip5", "county", "phone", "fax",
        "administrator", "npi_number", "license_number", "taxonomy_code",
        "entity_type", "bed_count", "estimated_waste_lbs_per_day",
        "estimated_monthly_volume", "waste_tier", "distance_from_birmingham",
        "service_zone", "completeness_score", "lead_score", "priority_tier",
        "status", "notes", "date_added",
    ]
    present = {k: v for k, v in lead_data.items() if k in fields and v is not None}
    cols = list(present.keys())
    vals = list(present.values())
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(cols)

    # Build SET clause for ON CONFLICT (skip lead_uid, first_seen)
    update_cols = [c for c in cols if c not in ("lead_uid", "date_added")]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    set_clause += ", last_updated = NOW()"

    sql = f"""
        INSERT INTO leads ({col_names})
        VALUES ({placeholders})
        ON CONFLICT (lead_uid) DO UPDATE SET {set_clause}
        RETURNING id
    """
    with get_cursor() as cur:
        cur.execute(sql, vals)
        return cur.fetchone()["id"]


def upsert_lead_source(lead_id, source, source_id, raw_data=None, confidence=None):
    """Insert or update a lead source attribution record."""
    sql = """
        INSERT INTO lead_sources (lead_id, source, source_id, raw_data, match_confidence)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (lead_id, source) DO UPDATE SET
            source_id = EXCLUDED.source_id,
            raw_data = EXCLUDED.raw_data,
            match_confidence = EXCLUDED.match_confidence,
            ingested_at = NOW()
    """
    raw_json = json.dumps(raw_data) if raw_data else None
    with get_cursor() as cur:
        cur.execute(sql, (lead_id, source, source_id, raw_json, confidence))


def record_score_history(lead_id, score, tier, breakdown):
    """Record a lead score snapshot."""
    sql = """
        INSERT INTO lead_score_history (lead_id, score, priority_tier, score_breakdown)
        VALUES (%s, %s, %s, %s)
    """
    with get_cursor() as cur:
        cur.execute(sql, (lead_id, score, tier, json.dumps(breakdown)))


def start_pipeline_run():
    """Create a new pipeline run record. Returns the run id."""
    sql = """
        INSERT INTO pipeline_runs (status) VALUES ('running') RETURNING id
    """
    with get_cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()["id"]


def finish_pipeline_run(run_id, status, stage_results, new_leads, updated_leads, total_leads, error_log=None):
    """Update a pipeline run record on completion."""
    sql = """
        UPDATE pipeline_runs
        SET completed_at = NOW(), status = %s, stage_results = %s,
            new_leads = %s, updated_leads = %s, total_leads = %s, error_log = %s
        WHERE id = %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (status, json.dumps(stage_results), new_leads, updated_leads, total_leads, error_log, run_id))


def run_migration(migration_file):
    """Execute a SQL migration file."""
    with open(migration_file, "r") as f:
        sql = f.read()
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
        print(f"Migration applied: {os.path.basename(migration_file)}")
    except Exception as e:
        conn.rollback()
        print(f"Migration failed: {e}")
        raise
    finally:
        cur.close()


def close():
    """Close the database connection."""
    global _conn
    if _conn and not _conn.closed:
        _conn.close()
        _conn = None
