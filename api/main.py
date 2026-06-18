"""
main.py
=======
FastAPI app serving EU energy price data from PostgreSQL (energy_data schema).

Endpoints
---------
  GET /                  Welcome message and endpoint index
  GET /health            DB connection status + row counts
  GET /prices            mart_energy_prices with optional filters
  GET /prices/summary    Average price per country for the latest year
  GET /prices/yoy        YoY change per country sorted by biggest change

Run
---
  uvicorn api.main:app --reload          (from project root)
  uvicorn api.main:app --port 8000       (default port)
"""

from contextlib import asynccontextmanager
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_CONFIG = dict(
    host   = "127.0.0.1",
    port   = 5433,
    dbname = "eu_energy",
    user   = "rahulaswani",
)

SCHEMA = "energy_data"
STG    = f"{SCHEMA}.stg_electricity_prices"
MART   = f"{SCHEMA}.mart_energy_prices"


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_connection() -> psycopg2.extensions.connection:
    """Open a new psycopg2 connection. Raises HTTPException on failure."""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}")


def fetchall_as_dicts(cur, query: str, params=None) -> list[dict]:
    """Execute query and return rows as a list of dicts."""
    cur.execute(query, params or ())
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetchone_value(cur, query: str, params=None):
    """Execute query and return the first column of the first row."""
    cur.execute(query, params or ())
    row = cur.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# App + lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Smoke-test the DB connection on startup; log a warning if unavailable
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        print(f"[startup] Connected to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    except psycopg2.OperationalError as exc:
        print(f"[startup] WARNING: DB not reachable — {exc}")
    yield


app = FastAPI(
    title       = "EU Energy Prices API",
    description = "Serves electricity price data for six EU countries (2015–2024).",
    version     = "1.0.0",
    lifespan    = lifespan,
)


# ---------------------------------------------------------------------------
# GET /
# ---------------------------------------------------------------------------

@app.get("/", summary="Welcome")
def root():
    return {
        "message": "EU Energy Prices API",
        "version": "1.0.0",
        "endpoints": [
            {"method": "GET", "path": "/",               "description": "This welcome message"},
            {"method": "GET", "path": "/health",         "description": "Database connection status and row counts"},
            {"method": "GET", "path": "/prices",         "description": "Mart prices with optional filters: country, year, consumer_type"},
            {"method": "GET", "path": "/prices/summary", "description": "Average price per country for the latest available year"},
            {"method": "GET", "path": "/prices/yoy",     "description": "Year-over-year change per country, sorted by biggest change"},
        ],
    }


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

@app.get("/health", summary="Database health check")
def health():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()

        stg_count  = fetchone_value(cur, f"SELECT COUNT(*) FROM {STG}")
        mart_count = fetchone_value(cur, f"SELECT COUNT(*) FROM {MART}")
        latest     = fetchone_value(cur, f"SELECT MAX(period) FROM {STG}")

        cur.close()
        conn.close()

        return {
            "status": "ok",
            "database": f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}",
            "tables": {
                STG:  {"rows": stg_count},
                MART: {"rows": mart_count},
            },
            "latest_period": latest,
        }

    except psycopg2.OperationalError as exc:
        return JSONResponse(
            status_code=503,
            content={"status": "unavailable", "detail": str(exc)},
        )


# ---------------------------------------------------------------------------
# GET /prices
# ---------------------------------------------------------------------------

@app.get("/prices", summary="Mart energy prices")
def prices(
    country:       Optional[str] = Query(None, description="ISO alpha-2 country code, e.g. DE"),
    year:          Optional[int] = Query(None, description="Calendar year, e.g. 2022"),
    consumer_type: Optional[str] = Query(None, description="household or industry"),
):
    if consumer_type and consumer_type not in ("household", "industry"):
        raise HTTPException(
            status_code=422,
            detail="consumer_type must be 'household' or 'industry'",
        )

    clauses = []
    params  = []

    if country:
        clauses.append("country_code = %s")
        params.append(country.upper())
    if year:
        clauses.append("year = %s")
        params.append(year)
    if consumer_type:
        clauses.append("consumer_type = %s")
        params.append(consumer_type)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    query = f"""
        SELECT
            country_code, country_name, year, consumer_type,
            avg_price_eur_per_kwh, min_price, max_price,
            price_category_mode, prior_year_avg_price, yoy_change_pct
        FROM   {MART}
        {where}
        ORDER  BY country_code, consumer_type, year
    """

    conn = get_connection()
    try:
        cur  = conn.cursor()
        rows = fetchall_as_dicts(cur, query, params)
        cur.close()
    finally:
        conn.close()

    return {"count": len(rows), "filters": {"country": country, "year": year, "consumer_type": consumer_type}, "data": rows}


# ---------------------------------------------------------------------------
# GET /prices/summary
# ---------------------------------------------------------------------------

@app.get("/prices/summary", summary="Average price per country for the latest year")
def prices_summary():
    query = f"""
        WITH latest_year AS (
            SELECT MAX(year) AS yr FROM {MART}
        )
        SELECT
            m.country_code,
            m.country_name,
            m.year,
            m.consumer_type,
            m.avg_price_eur_per_kwh,
            m.price_category_mode
        FROM   {MART} m
        JOIN   latest_year l ON m.year = l.yr
        ORDER  BY m.country_code, m.consumer_type
    """

    conn = get_connection()
    try:
        cur  = conn.cursor()
        rows = fetchall_as_dicts(cur, query)
        year = rows[0]["year"] if rows else None
        cur.close()
    finally:
        conn.close()

    return {"year": year, "count": len(rows), "data": rows}


# ---------------------------------------------------------------------------
# GET /prices/yoy
# ---------------------------------------------------------------------------

@app.get("/prices/yoy", summary="Year-over-year change sorted by biggest change")
def prices_yoy():
    query = f"""
        SELECT
            country_code, country_name, year, consumer_type,
            avg_price_eur_per_kwh, prior_year_avg_price, yoy_change_pct
        FROM   {MART}
        WHERE  yoy_change_pct IS NOT NULL
        ORDER  BY yoy_change_pct DESC
    """

    conn = get_connection()
    try:
        cur  = conn.cursor()
        rows = fetchall_as_dicts(cur, query)
        cur.close()
    finally:
        conn.close()

    return {"count": len(rows), "data": rows}
