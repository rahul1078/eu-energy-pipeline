"""
run_pipeline.py
===============
Orchestrates the full EU energy pipeline in four sequential steps:

  1. Fetch    — ingestion/scripts/fetch_eurostat.py
                Downloads electricity price data from the Eurostat REST API
                and writes data/raw/eurostat_electricity_prices.csv.

  2. Load     — ingestion/scripts/load_to_duckdb.py
                Reads the CSV and loads it into data/energy.duckdb as the
                table raw_electricity_prices (skips if already loaded).

  3. dbt run  — builds the staging view and mart table inside energy_eu/.

  4. dbt test — runs all schema data-quality tests inside energy_eu/.

Each step streams its output live to the terminal.  If any step exits with a
non-zero return code the pipeline stops immediately, prints which step failed,
and exits with that step's return code.  A summary table is always printed at
the end, including any steps that were skipped because a prior step failed.

Usage
-----
  python pipeline/run_pipeline.py
"""

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

# ---------------------------------------------------------------------------
# Paths — resolved from this file's location so the script works from any cwd
# ---------------------------------------------------------------------------

_SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT  = os.path.abspath(os.path.join(_SCRIPT_DIR, ".."))
ENERGY_EU_DIR = os.path.join(PROJECT_ROOT, "energy_eu")


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class StepDef:
    """Static definition of a single pipeline step."""
    name: str
    cmd:  list[str]
    cwd:  str


@dataclass
class StepResult:
    """Outcome recorded after a step runs (or is skipped)."""
    name:       str
    status:     str               # "PASS" | "FAIL" | "SKIP"
    started_at: Optional[datetime] = None
    ended_at:   Optional[datetime] = None
    returncode: Optional[int]      = None

    @property
    def duration_s(self) -> Optional[float]:
        if self.started_at and self.ended_at:
            return (self.ended_at - self.started_at).total_seconds()
        return None


# ---------------------------------------------------------------------------
# Step definitions
# ---------------------------------------------------------------------------

def build_steps() -> list[StepDef]:
    """
    Construct the ordered list of pipeline steps.

    Uses sys.executable so all Python steps share the same interpreter
    that is running this script.  Uses shutil.which for dbt so the PATH
    is resolved at runtime rather than hard-coded.
    """
    dbt = shutil.which("dbt")
    if dbt is None:
        _abort("'dbt' executable not found on PATH. "
               "Activate the conda / venv environment that contains dbt.")

    ingestion = os.path.join(PROJECT_ROOT, "ingestion", "scripts")

    return [
        StepDef(
            name="Fetch Eurostat data",
            cmd=[sys.executable, os.path.join(ingestion, "fetch_eurostat.py")],
            cwd=PROJECT_ROOT,
        ),
        StepDef(
            name="Load to DuckDB",
            cmd=[sys.executable, os.path.join(ingestion, "load_to_duckdb.py")],
            cwd=PROJECT_ROOT,
        ),
        StepDef(
            name="dbt run",
            cmd=[dbt, "run", "--profiles-dir", "."],
            cwd=ENERGY_EU_DIR,
        ),
        StepDef(
            name="dbt test",
            cmd=[dbt, "test", "--profiles-dir", "."],
            cwd=ENERGY_EU_DIR,
        ),
    ]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_step(step_num: int, total: int, step: StepDef) -> StepResult:
    """
    Execute a single pipeline step, streaming its output directly to the
    terminal.  Returns a StepResult with timing and exit code.
    """
    _section(f"Step {step_num}/{total}: {step.name}")

    started_at = datetime.now()
    print(f"  Started : {_fmt(started_at)}")
    print()

    # stdout/stderr stream to the terminal unchanged (no capture).
    # The return code is the only thing we read back here.
    proc = subprocess.run(step.cmd, cwd=step.cwd)

    ended_at = datetime.now()
    print()
    print(f"  Ended   : {_fmt(ended_at)}")
    print(f"  Duration: {_elapsed(started_at, ended_at)}")

    status = "PASS" if proc.returncode == 0 else "FAIL"
    return StepResult(
        name=step.name,
        status=status,
        started_at=started_at,
        ended_at=ended_at,
        returncode=proc.returncode,
    )


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(results: list[StepResult]) -> None:
    """Print a table showing every step's status and duration."""
    _section("Pipeline Summary")

    col_name  = 32
    col_status = 6
    col_dur   = 10

    header = (
        f"  {'Step':<{col_name}} {'Status':^{col_status}} {'Duration':>{col_dur}}"
    )
    divider = f"  {'-' * col_name} {'-' * col_status} {'-' * col_dur}"

    print(header)
    print(divider)

    total_s = 0.0
    for i, r in enumerate(results, start=1):
        dur_s  = r.duration_s or 0.0
        total_s += dur_s
        dur_str = f"{dur_s:.1f}s" if r.duration_s is not None else "—"
        label   = f"{i}. {r.name}"
        print(f"  {label:<{col_name}} {r.status:^{col_status}} {dur_str:>{col_dur}}")

    print(divider)
    print(f"  {'Total':<{col_name}} {'':^{col_status}} {total_s:>{col_dur-1}.1f}s")
    print()

    passed  = sum(1 for r in results if r.status == "PASS")
    failed  = [r for r in results if r.status == "FAIL"]
    skipped = sum(1 for r in results if r.status == "SKIP")

    if not failed:
        print(f"  Result : ALL {passed} STEPS PASSED")
    else:
        print(f"  Result : FAILED at '{failed[0].name}'"
              + (f"  ({skipped} step(s) skipped)" if skipped else ""))
    print()


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _elapsed(start: datetime, end: datetime) -> str:
    return f"{(end - start).total_seconds():.1f}s"


def _section(title: str) -> None:
    width = 60
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def _abort(msg: str) -> None:
    print(f"\nERROR: {msg}")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    steps = build_steps()
    total = len(steps)

    _section(f"EU Energy Pipeline  —  {_fmt(datetime.now())}")
    print(f"  Project root : {PROJECT_ROOT}")
    print(f"  Steps        : {total}")

    results: list[StepResult] = []

    for i, step in enumerate(steps, start=1):
        result = run_step(i, total, step)
        results.append(result)

        if result.status == "FAIL":
            # Mark every remaining step as skipped
            for skipped_step in steps[i:]:
                results.append(StepResult(name=skipped_step.name, status="SKIP"))

            print()
            print(
                f"ERROR: Step {i}/{total} '{step.name}' failed "
                f"(exit code {result.returncode})."
            )
            print("Pipeline halted. Check the output above for details.")
            print_summary(results)
            sys.exit(result.returncode)

    print_summary(results)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user (KeyboardInterrupt).")
        sys.exit(130)
