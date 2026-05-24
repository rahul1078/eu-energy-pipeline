"""
load_to_duckdb.py
=================
Reads the cleaned Eurostat electricity price CSV and loads it into a local
DuckDB database as the table ``raw_electricity_prices``.

Idempotency
-----------
If the table already exists the script prints its current row count and exits
without touching the database.  Drop the table manually (or delete the .duckdb
file) to force a fresh load.

Source  : data/raw/eurostat_electricity_prices.csv
Target  : data/energy.duckdb  →  table raw_electricity_prices

Added column
------------
  loaded_at  TIMESTAMPTZ   — UTC timestamp recorded at load time, so every row
                             carries a provenance marker for lineage tracking.

Usage
-----
  python ingestion/scripts/load_to_duckdb.py
"""

import os
import sys

import duckdb

# ---------------------------------------------------------------------------
# Paths — resolved relative to this script so the script works from any cwd
# ---------------------------------------------------------------------------

# This file lives at  ingestion/scripts/load_to_duckdb.py
# Two levels up is the project root.
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

CSV_PATH  = os.path.join(_PROJECT_ROOT, "data", "raw", "eurostat_electricity_prices.csv")
DB_PATH   = os.path.join(_PROJECT_ROOT, "data", "energy.duckdb")
TABLE     = "raw_electricity_prices"


# ---------------------------------------------------------------------------
# Helper — check whether the target table already exists
# ---------------------------------------------------------------------------

def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """
    Query the information_schema to check if *table_name* exists in the
    current database.  Using information_schema is portable and avoids
    catching exceptions as control flow.
    """
    result = con.execute(
        """
        SELECT COUNT(*)
        FROM   information_schema.tables
        WHERE  table_name = ?
        """,
        [table_name],
    ).fetchone()
    return result[0] > 0  # type: ignore[index]


# ---------------------------------------------------------------------------
# Helper — print the table schema for visibility
# ---------------------------------------------------------------------------

def print_schema(con: duckdb.DuckDBPyConnection, table_name: str) -> None:
    """Print column names and data types for *table_name*."""
    cols = con.execute(
        """
        SELECT column_name, data_type
        FROM   information_schema.columns
        WHERE  table_name = ?
        ORDER  BY ordinal_position
        """,
        [table_name],
    ).fetchall()
    print(f"\n  Schema of '{table_name}':")
    print(f"  {'Column':<30} {'Type'}")
    print(f"  {'-'*30} {'-'*20}")
    for col_name, data_type in cols:
        print(f"  {col_name:<30} {data_type}")
    print()


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("  Eurostat → DuckDB Loader")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Pre-flight: confirm the source CSV is present before opening the DB
    # ------------------------------------------------------------------
    if not os.path.exists(CSV_PATH):
        print(f"\nERROR: Source CSV not found:\n  {CSV_PATH}")
        print("Run ingestion/scripts/fetch_eurostat.py first.")
        sys.exit(1)

    csv_size_kb = os.path.getsize(CSV_PATH) / 1024
    print(f"\nSource : {CSV_PATH}")
    print(f"         ({csv_size_kb:,.1f} KB on disk)")
    print(f"Target : {DB_PATH}")
    print(f"Table  : {TABLE}")

    # ------------------------------------------------------------------
    # Open (or create) the DuckDB database file
    # DuckDB creates the file automatically if it does not yet exist.
    # ------------------------------------------------------------------
    print(f"\nConnecting to DuckDB…")
    con = duckdb.connect(DB_PATH)

    try:
        # --------------------------------------------------------------
        # Idempotency guard — skip if the table already exists
        # --------------------------------------------------------------
        if table_exists(con, TABLE):
            existing_rows = con.execute(
                f"SELECT COUNT(*) FROM {TABLE}"
            ).fetchone()[0]  # type: ignore[index]

            print(
                f"\nTable '{TABLE}' already exists "
                f"({existing_rows:,} rows) — skipping load.\n"
                f"Drop the table or delete {DB_PATH} to force a fresh load."
            )
            print_schema(con, TABLE)
            return

        # --------------------------------------------------------------
        # Load: use DuckDB's native read_csv_auto() for fast, type-safe
        # ingestion directly from the file path (no Python row iteration).
        #
        # DuckDB infers column types automatically from the CSV.  The only
        # column that needs a nudge is value_eur_per_kwh (DOUBLE) — but
        # DuckDB detects this correctly from the numeric values.
        #
        # now()::TIMESTAMP strips the timezone offset and stores a plain
        # TIMESTAMP, which serialises back to Python without needing pytz.
        # Every row in the same load shares the same timestamp, making it
        # easy to query "what was loaded in this batch".
        # --------------------------------------------------------------
        print(f"\nLoading CSV into '{TABLE}'…")

        con.execute(f"""
            CREATE TABLE {TABLE} AS
            SELECT
                *,
                now()::TIMESTAMP AS loaded_at
            FROM read_csv_auto(
                '{CSV_PATH}',
                header      = true,
                nullstr     = '',
                auto_detect = true
            )
        """)

        # --------------------------------------------------------------
        # Confirm the load and report results
        # --------------------------------------------------------------
        row_count = con.execute(
            f"SELECT COUNT(*) FROM {TABLE}"
        ).fetchone()[0]  # type: ignore[index]

        loaded_at = con.execute(
            f"SELECT loaded_at FROM {TABLE} LIMIT 1"
        ).fetchone()[0]  # type: ignore[index]

        print(f"\n  ✓ {row_count:,} rows loaded successfully.")
        print(f"  ✓ loaded_at timestamp : {loaded_at}")

        # Show the schema so the next engineer knows exactly what landed
        print_schema(con, TABLE)

        # Spot-check: print a tiny sample for a quick sanity check
        print("  Sample rows (Germany, household, total band, EUR, all taxes):")
        sample = con.execute(f"""
            SELECT consumer_type, country_name, time,
                   nrg_cons, tax, currency, value_eur_per_kwh
            FROM   {TABLE}
            WHERE  geo           = 'DE'
              AND  consumer_type = 'household'
              AND  nrg_cons      = 'TOT_KWH'
              AND  currency      = 'EUR'
              AND  tax           = 'I_TAX'
            ORDER  BY time
            LIMIT  5
        """).fetchall()
        header = ("consumer_type", "country", "time", "band", "tax", "currency", "€/kWh")
        fmt = "  {:<14} {:<12} {:<10} {:<10} {:<7} {:<10} {}"
        print(fmt.format(*header))
        print("  " + "-" * 75)
        for row in sample:
            print(fmt.format(*row))

        print(f"\n  Database saved to: {os.path.abspath(DB_PATH)}")
        print("\nDone! ✓")

    finally:
        # Always close the connection, even if an exception was raised
        con.close()


if __name__ == "__main__":
    main()
