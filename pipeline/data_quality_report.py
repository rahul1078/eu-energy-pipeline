"""
data_quality_report.py
======================
Connects to the eu_energy PostgreSQL database and prints a data health
summary covering row counts, latest period, per-country record counts,
null checks, price anomaly detection, and a year-over-year highlights table.

Usage
-----
  python pipeline/data_quality_report.py
"""

import sys
from datetime import datetime

import psycopg2

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PG_HOST  = "127.0.0.1"
PG_PORT  = 5433
PG_DB    = "eu_energy"
PG_USER  = "rahulaswani"

SCHEMA = "energy_data"
STG    = f"{SCHEMA}.stg_electricity_prices"
MART   = f"{SCHEMA}.mart_energy_prices"

PRICE_MIN = 0.05   # EUR/kWh — below this is anomalous
PRICE_MAX = 1.00   # EUR/kWh — above this is anomalous


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _section(title: str) -> None:
    width = 60
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def _bar(width: int = 60) -> None:
    print(f"  {'-' * (width - 2)}")


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _abort(msg: str) -> None:
    print(f"\nERROR: {msg}")
    sys.exit(1)


def _tbl_row(cols: list, widths: list) -> str:
    """Format a fixed-width table row. Values are left-aligned, last column right-aligned."""
    parts = []
    for i, (val, w) in enumerate(zip(cols, widths)):
        s = str(val) if val is not None else "—"
        if i == len(widths) - 1:
            parts.append(f"{s:>{w}}")
        else:
            parts.append(f"{s:<{w}}")
    return "  " + "  ".join(parts)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def connect() -> psycopg2.extensions.connection:
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
        )
        return conn
    except psycopg2.OperationalError as exc:
        _abort(
            f"Cannot connect to PostgreSQL at {PG_HOST}:{PG_PORT}/{PG_DB}\n  {exc}"
        )


# ---------------------------------------------------------------------------
# Check 1 — Row counts
# ---------------------------------------------------------------------------

def check_row_counts(cur) -> None:
    cur.execute(f"SELECT COUNT(*) FROM {STG}")
    stg_count = cur.fetchone()[0]

    cur.execute(f"SELECT COUNT(*) FROM {MART}")
    mart_count = cur.fetchone()[0]

    stg_flag  = "[WARN]" if stg_count == 0  else "[OK]  "
    mart_flag = "[WARN]" if mart_count == 0 else "[OK]  "

    print(f"  {stg_flag} {STG:<42} {stg_count:>7,} rows")
    print(f"  {mart_flag} {MART:<42} {mart_count:>7,} rows")


# ---------------------------------------------------------------------------
# Check 2 — Latest period
# ---------------------------------------------------------------------------

def check_latest_period(cur) -> None:
    cur.execute(f"SELECT MAX(period) FROM {STG}")
    latest = cur.fetchone()[0]

    if latest is None:
        print(f"  [WARN] Latest period: no data found")
    else:
        print(f"  [OK]   Latest period: {latest}")


# ---------------------------------------------------------------------------
# Check 3 — Records per country (mart)
# ---------------------------------------------------------------------------

def check_records_per_country(cur) -> None:
    cur.execute(f"""
        SELECT country_code, country_name, COUNT(*) AS rows
        FROM   {MART}
        GROUP  BY country_code, country_name
        ORDER  BY country_code
    """)
    rows = cur.fetchall()

    widths = [8, 16, 6]
    print(_tbl_row(["Code", "Country", "Rows"], widths))
    _bar()
    for code, name, n in rows:
        print(_tbl_row([code, name, f"{n:,}"], widths))

    if not rows:
        print("  [WARN] No rows in mart table.")


# ---------------------------------------------------------------------------
# Check 4 — Null checks on key columns
# ---------------------------------------------------------------------------

def check_nulls(cur) -> None:
    checks = [
        (STG,  "country_code"),
        (STG,  "period"),
        (STG,  "price_eur_per_kwh"),
        (MART, "country_code"),
        (MART, "year"),
        (MART, "avg_price_eur_per_kwh"),
    ]

    any_fail = False
    for table, column in checks:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
        null_count = cur.fetchone()[0]

        if null_count > 0:
            flag = "[FAIL]"
            any_fail = True
        else:
            flag = "[OK]  "

        label = f"{table}.{column}"
        print(f"  {flag} {label:<52}  {null_count:>4} nulls")

    if not any_fail:
        print()
        print("  All key columns are null-free.")


# ---------------------------------------------------------------------------
# Check 5 — Price range anomaly detection
# ---------------------------------------------------------------------------

def check_price_range(cur) -> None:
    cur.execute(f"""
        SELECT country_code, consumer_type, year, avg_price_eur_per_kwh
        FROM   {MART}
        WHERE  avg_price_eur_per_kwh < %s
           OR  avg_price_eur_per_kwh > %s
        ORDER  BY country_code, consumer_type, year
    """, (PRICE_MIN, PRICE_MAX))
    anomalies = cur.fetchall()

    print(f"  Threshold: {PRICE_MIN} – {PRICE_MAX} EUR/kWh")
    print()

    if not anomalies:
        print("  [OK]   No price anomalies detected.")
    else:
        print(f"  [WARN] {len(anomalies)} anomalous record(s) found:")
        print()
        widths = [8, 12, 6, 10]
        print(_tbl_row(["Country", "Consumer", "Year", "Avg Price"], widths))
        _bar()
        for code, consumer, year, price in anomalies:
            print(_tbl_row([code, consumer, year, f"{price:.4f}"], widths))


# ---------------------------------------------------------------------------
# Check 6 — YoY change highlights
# ---------------------------------------------------------------------------

def check_yoy_summary(cur) -> None:
    widths = [8, 12, 6, 12]
    header = _tbl_row(["Country", "Consumer", "Year", "YoY Change %"], widths)

    # Top 3 increases
    cur.execute(f"""
        SELECT country_code, consumer_type, year, yoy_change_pct
        FROM   {MART}
        WHERE  yoy_change_pct IS NOT NULL
        ORDER  BY yoy_change_pct DESC
        LIMIT  3
    """)
    increases = cur.fetchall()

    print("  Biggest price increases:")
    print(header)
    _bar()
    for code, consumer, year, pct in increases:
        print(_tbl_row([code, consumer, year, f"+{pct:.2f}%"], widths))

    print()

    # Top 3 decreases
    cur.execute(f"""
        SELECT country_code, consumer_type, year, yoy_change_pct
        FROM   {MART}
        WHERE  yoy_change_pct IS NOT NULL
        ORDER  BY yoy_change_pct ASC
        LIMIT  3
    """)
    decreases = cur.fetchall()

    print("  Biggest price decreases:")
    print(header)
    _bar()
    for code, consumer, year, pct in decreases:
        print(_tbl_row([code, consumer, year, f"{pct:.2f}%"], widths))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    _section(f"EU Energy Data Quality Report  —  {_fmt(datetime.now())}")
    print(f"  Database : {PG_HOST}:{PG_PORT}/{PG_DB}")
    print(f"  Schema   : {SCHEMA}")

    conn = connect()
    cur  = conn.cursor()

    _section("1. Row Counts")
    check_row_counts(cur)

    _section("2. Latest Period")
    check_latest_period(cur)

    _section("3. Records per Country (mart)")
    check_records_per_country(cur)

    _section("4. Null Checks on Key Columns")
    check_nulls(cur)

    _section("5. Price Range Anomaly Detection")
    check_price_range(cur)

    _section("6. Year-over-Year Change Highlights")
    check_yoy_summary(cur)

    cur.close()
    conn.close()

    _section(f"Report complete  —  {_fmt(datetime.now())}")


main()
