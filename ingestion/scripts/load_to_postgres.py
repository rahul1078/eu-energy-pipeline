"""
load_to_postgres.py
===================
Reads stg_electricity_prices and mart_energy_prices from the DuckDB database
at data/energy.duckdb and loads them into PostgreSQL under the energy_data schema.

Target database
---------------
  Host     : 127.0.0.1
  Port     : 5433
  Database : eu_energy
  User     : rahulaswani
  Schema   : energy_data

Both tables are replaced on each run (if_exists='replace'), so the script is
safe to re-run after a pipeline refresh.

Usage
-----
  python ingestion/scripts/load_to_postgres.py
"""

import os
import sys

import duckdb
import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", ".."))

DUCKDB_PATH  = os.path.join(PROJECT_ROOT, "data", "energy.duckdb")

PG_HOST      = "127.0.0.1"
PG_PORT      = 5433
PG_DB        = "eu_energy"
PG_USER      = "rahulaswani"
PG_PASSWORD  = ""          # no password
PG_SCHEMA    = "energy_data"

TABLES = [
    # (duckdb_query, postgres_table_name)
    ("SELECT * FROM stg_electricity_prices", "stg_electricity_prices"),
    ("SELECT * FROM mart_energy_prices",     "mart_energy_prices"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _section(title: str) -> None:
    width = 60
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def ensure_schema(pg_conn_str: str, schema: str) -> None:
    """Create the target schema if it does not already exist."""
    conn = psycopg2.connect(pg_conn_str)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(schema)
            )
        )
    conn.close()
    print(f"  Schema '{schema}' ready.")


def read_from_duckdb(duckdb_path: str, query: str) -> pd.DataFrame:
    """Open a read-only DuckDB connection and run a query, returning a DataFrame."""
    con = duckdb.connect(duckdb_path, read_only=True)
    df  = con.execute(query).df()
    con.close()
    return df


def load_to_postgres(
    df: pd.DataFrame,
    engine,
    schema: str,
    table: str,
) -> int:
    """Write a DataFrame to PostgreSQL, replacing the table if it exists."""
    df.to_sql(
        name      = table,
        con       = engine,
        schema    = schema,
        if_exists = "replace",
        index     = False,
        method    = "multi",   # batches rows in a single INSERT per chunk
        chunksize = 1000,
    )
    return len(df)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    _section("Load DuckDB → PostgreSQL")

    # Build connection strings
    if PG_PASSWORD:
        pg_conn_str = (
            f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} "
            f"user={PG_USER} password={PG_PASSWORD}"
        )
        pg_url = (
            f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}"
            f"@{PG_HOST}:{PG_PORT}/{PG_DB}"
        )
    else:
        pg_conn_str = (
            f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER}"
        )
        pg_url = (
            f"postgresql+psycopg2://{PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB}"
        )

    print(f"  DuckDB   : {DUCKDB_PATH}")
    print(f"  Postgres : {PG_HOST}:{PG_PORT}/{PG_DB}  (schema: {PG_SCHEMA})")

    # Verify DuckDB file exists
    if not os.path.exists(DUCKDB_PATH):
        print(f"\nERROR: DuckDB file not found: {DUCKDB_PATH}")
        print("Run the pipeline first:  python pipeline/run_pipeline.py")
        sys.exit(1)

    # Ensure target schema exists
    try:
        ensure_schema(pg_conn_str, PG_SCHEMA)
    except psycopg2.OperationalError as exc:
        print(f"\nERROR: Cannot connect to PostgreSQL: {exc}")
        sys.exit(1)

    # SQLAlchemy engine for pandas .to_sql()
    engine = create_engine(pg_url)

    total_rows = 0

    for query, table_name in TABLES:
        print()
        print(f"  Table: {PG_SCHEMA}.{table_name}")

        # Read from DuckDB
        print(f"    Reading from DuckDB ...", end="", flush=True)
        try:
            df = read_from_duckdb(DUCKDB_PATH, query)
        except Exception as exc:
            print(f"\nERROR: Failed to read '{table_name}' from DuckDB: {exc}")
            sys.exit(1)
        print(f" {len(df):,} rows, {len(df.columns)} columns")

        # Load into Postgres
        print(f"    Loading into PostgreSQL ...", end="", flush=True)
        try:
            n = load_to_postgres(df, engine, PG_SCHEMA, table_name)
        except Exception as exc:
            print(f"\nERROR: Failed to load '{table_name}' into PostgreSQL: {exc}")
            sys.exit(1)
        print(f" done")
        print(f"    Rows loaded: {n:,}")
        total_rows += n

    engine.dispose()

    _section("Summary")
    print(f"  Tables loaded : {len(TABLES)}")
    print(f"  Total rows    : {total_rows:,}")
    print(f"  Destination   : {PG_HOST}:{PG_PORT}/{PG_DB}.{PG_SCHEMA}")
    print()


if __name__ == "__main__":
    main()
