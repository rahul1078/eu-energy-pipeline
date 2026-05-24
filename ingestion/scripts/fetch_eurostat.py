"""
fetch_eurostat.py
=================
Fetches electricity price data from the Eurostat REST API (no API key required).

Eurostat publishes bi-annual electricity prices (first half / second half of each
year) broken down by consumer type and consumption band.

Datasets used
-------------
  nrg_pc_204 — Electricity prices for *household* consumers
  nrg_pc_205 — Electricity prices for *non-household* (industrial) consumers

Both datasets share the same dimension structure:
  freq | siec | nrg_cons | unit | tax | currency | geo | time

API endpoint (no authentication needed):
  https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}

The response is SDMX-JSON 2.0 — dimension metadata and observed values are stored
separately; this script re-joins them into a flat, analysis-ready table.

Output
------
  data/raw/eurostat_electricity_prices.csv
"""

import os
import sys
import time
import requests
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Base URL for the Eurostat dissemination REST API (no key required)
BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

# Dataset codes and their human-readable consumer-type labels
DATASETS: dict[str, str] = {
    "nrg_pc_204": "household",
    "nrg_pc_205": "industry",
}

# ISO alpha-2 country codes as used by Eurostat, mapped to full country names
COUNTRIES: dict[str, str] = {
    "DE": "Germany",
    "FR": "France",
    "NL": "Netherlands",
    "ES": "Spain",
    "PL": "Poland",
    "IT": "Italy",
}

# Inclusive year range to keep after fetching (Eurostat returns all available years)
START_YEAR = 2015
END_YEAR   = 2024

# Where to write the final CSV file
# __file__ is  ingestion/scripts/fetch_eurostat.py  →  go up two levels to project root
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUTPUT_DIR    = os.path.join(_PROJECT_ROOT, "data", "raw")
OUTPUT_FILE   = os.path.join(OUTPUT_DIR, "eurostat_electricity_prices.csv")

# HTTP request settings
REQUEST_TIMEOUT  = 60   # seconds before giving up on a single request
RETRY_ATTEMPTS   = 3    # how many times to try each URL
RETRY_DELAY      = 5    # seconds to wait between retries


# ---------------------------------------------------------------------------
# Step 1 — Build the API request URL
# ---------------------------------------------------------------------------

def build_url(dataset_code: str, country_codes: list[str]) -> str:
    """
    Construct the Eurostat API URL for a given dataset filtered to specific countries.

    The API accepts repeated 'geo' query parameters to filter by multiple countries
    at once, which keeps the response payload small.

    Example output:
      https://.../nrg_pc_204?format=JSON&lang=EN&geo=DE&geo=FR&geo=NL&geo=ES&geo=PL&geo=IT
    """
    geo_params = "&".join(f"geo={code}" for code in country_codes)
    return f"{BASE_URL}/{dataset_code}?format=JSON&lang=EN&{geo_params}"


# ---------------------------------------------------------------------------
# Step 2 — Fetch JSON with retry logic
# ---------------------------------------------------------------------------

def fetch_json(url: str) -> dict:
    """
    Download and parse a JSON response from *url*, retrying on transient errors.

    Raises
    ------
    RuntimeError
        If all retry attempts fail for any reason.
    """
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            print(f"    [Attempt {attempt}/{RETRY_ATTEMPTS}] GET {url[:90]}...")
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            # Raise an exception for 4xx / 5xx HTTP status codes
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as exc:
            print(f"    HTTP error {exc.response.status_code}: {exc}")
        except requests.exceptions.ConnectionError as exc:
            print(f"    Connection error: {exc}")
        except requests.exceptions.Timeout:
            print(f"    Request timed out after {REQUEST_TIMEOUT}s.")

        # Wait before retrying (skip delay on the last attempt)
        if attempt < RETRY_ATTEMPTS:
            print(f"    Waiting {RETRY_DELAY}s before retry…")
            time.sleep(RETRY_DELAY)

    raise RuntimeError(
        f"Failed to fetch data after {RETRY_ATTEMPTS} attempt(s): {url}"
    )


# ---------------------------------------------------------------------------
# Step 3 — Parse the SDMX-JSON response into a flat DataFrame
# ---------------------------------------------------------------------------

def parse_sdmx_json(response: dict, consumer_label: str) -> pd.DataFrame:
    """
    Convert a Eurostat SDMX-JSON 2.0 response into a flat pandas DataFrame.

    SDMX-JSON background
    ~~~~~~~~~~~~~~~~~~~~
    The API separates *metadata* (dimension definitions) from *values*.  Each
    observed value is stored in ``response["value"]`` under a string key that
    represents a *flat* index into a multi-dimensional hypercube.

    The shape of that hypercube is given by ``response["size"]``, e.g.::

        id   = ["freq", "siec", "nrg_cons", "unit", "tax", "currency", "geo", "time"]
        size = [  1,      1,       6,          1,     3,       3,        6,    38   ]

    A flat index ``i`` maps to per-dimension positions via standard C-order
    (row-major) stride arithmetic:
        strides[k] = product(size[k+1:])
        position[k] = (i // strides[k]) % size[k]

    Parameters
    ----------
    response : dict
        Parsed JSON body from the Eurostat API.
    consumer_label : str
        Either "household" or "industry" — added as a column for clarity.

    Returns
    -------
    pd.DataFrame
        One row per observed value.  Missing observations (absent from the
        ``value`` dict) are silently omitted, matching Eurostat's own convention.
    """
    dim_ids   = response["id"]    # ordered list of dimension names
    dim_sizes = response["size"]  # number of categories per dimension
    dimensions = response["dimension"]

    # --- Build position-to-code lookups for each dimension ---
    # category["index"]  maps  code → integer position
    # We invert it to     position → code
    pos_to_code: dict[str, dict[int, str]] = {}
    pos_to_label: dict[str, dict[int, str]] = {}
    for dim_id in dim_ids:
        cat = dimensions[dim_id]["category"]
        pos_to_code[dim_id]  = {pos: code  for code, pos  in cat["index"].items()}
        pos_to_label[dim_id] = {pos: label for code, label in cat["label"].items()
                                 for pos in [cat["index"].get(code, -1)]}

    # --- Pre-compute C-order strides ---
    # strides[k] = product of sizes for all dimensions to the right of k
    strides: list[int] = []
    running_stride = 1
    for size in reversed(dim_sizes):
        strides.insert(0, running_stride)
        running_stride *= size

    # --- Decode each observed value ---
    rows: list[dict] = []
    for flat_idx_str, value in response.get("value", {}).items():
        flat_idx = int(flat_idx_str)

        # Recover per-dimension position indices from the flat index
        row: dict = {}
        for dim_id, stride, size in zip(dim_ids, strides, dim_sizes):
            pos = (flat_idx // stride) % size
            row[dim_id]               = pos_to_code[dim_id].get(pos, f"pos{pos}")
            row[f"{dim_id}_label"]    = pos_to_label[dim_id].get(pos, "")

        row["value_eur_per_kwh"] = value   # unit is always KWH; currency varies
        row["consumer_type"]     = consumer_label
        rows.append(row)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Step 4 — Filter to the requested year range
# ---------------------------------------------------------------------------

def filter_year_range(df: pd.DataFrame, start: int, end: int) -> pd.DataFrame:
    """
    Retain only rows whose time period falls within [start, end] (inclusive).

    Eurostat bi-annual period codes look like  '2020-S1'  or  '2020-S2'.
    The year is always the first four characters.
    """
    if df.empty or "time" not in df.columns:
        return df

    # Extract the 4-digit year from strings like "2020-S1"
    years = df["time"].str[:4].astype(int, errors="ignore")
    mask  = (years >= start) & (years <= end)
    return df.loc[mask].copy()


# ---------------------------------------------------------------------------
# Step 5 — Tidy up and write the final CSV
# ---------------------------------------------------------------------------

def tidy_and_save(frames: list[pd.DataFrame]) -> None:
    """
    Concatenate all per-dataset DataFrames, add convenience columns, reorder
    columns so key identifiers come first, and write the result to CSV.
    """
    print("\nCombining all datasets…")
    combined = pd.concat(frames, ignore_index=True)

    # Add a human-readable country name alongside the ISO code
    combined.insert(0, "country_name", combined["geo"].map(COUNTRIES))

    # Move the most important identifiers to the front for readability
    priority = ["consumer_type", "geo", "country_name", "time"]
    remaining = [c for c in combined.columns if c not in priority]
    combined  = combined[priority + remaining]

    # Sort for reproducibility
    combined.sort_values(
        ["consumer_type", "geo", "time", "nrg_cons", "tax", "currency"],
        inplace=True,
        ignore_index=True,
    )

    print(f"Total rows  : {len(combined):,}")
    print(f"Columns     : {list(combined.columns)}")

    # Ensure the output directory exists before writing
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    combined.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved → {os.path.abspath(OUTPUT_FILE)}")


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 65)
    print("  Eurostat Electricity Price Fetcher")
    print("=" * 65)
    print(f"  Countries : {', '.join(COUNTRIES.values())}")
    print(f"  Years     : {START_YEAR}–{END_YEAR}")
    print(f"  Datasets  : {', '.join(DATASETS.keys())}")
    print("=" * 65)

    collected_frames: list[pd.DataFrame] = []

    for dataset_code, consumer_label in DATASETS.items():
        print(f"\n[{consumer_label.upper()}] Fetching '{dataset_code}'…")

        url = build_url(dataset_code, list(COUNTRIES.keys()))

        # --- Fetch ---
        try:
            raw = fetch_json(url)
        except RuntimeError as exc:
            print(f"  ERROR: {exc}")
            print(f"  Skipping dataset '{dataset_code}'.")
            continue

        # --- Parse ---
        print(f"    Parsing SDMX-JSON response…")
        df = parse_sdmx_json(raw, consumer_label)

        if df.empty:
            print(f"  WARNING: No observations returned for '{dataset_code}'. Skipping.")
            continue

        print(f"    Rows before year filter : {len(df):,}")

        # --- Filter years ---
        df = filter_year_range(df, START_YEAR, END_YEAR)
        print(f"    Rows after year filter  : {len(df):,}")

        collected_frames.append(df)
        print(f"  ✓ '{dataset_code}' processed successfully.")

    # --- Combine & save ---
    if not collected_frames:
        print("\nERROR: No data was fetched successfully. Exiting.")
        sys.exit(1)

    tidy_and_save(collected_frames)
    print("\nAll done! ✓")


if __name__ == "__main__":
    main()
