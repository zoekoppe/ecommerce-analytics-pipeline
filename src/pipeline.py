"""Prefect flow that orchestrates the e-commerce ELT pipeline.

Runs the full sequence, on demand or on a schedule:
    1. Ingest the product catalog from the DummyJSON API into BigQuery
    2. Build the dbt models
    3. Run the dbt data-quality tests

If any step fails, the run stops and the failure hook fires.

Run once, right now:
    uv run python src/pipeline.py

Serve it on a schedule (daily at 6am); leave this running:
    uv run python src/pipeline.py serve
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from prefect import flow, task
from prefect.logging import get_run_logger

# Paths are resolved relative to this file, so the flow works from anywhere.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
ANALYTICS_DIR = PROJECT_ROOT / "analytics"
INGEST_SCRIPT = PROJECT_ROOT / "src" / "ingest_products.py"


def _run(command: list[str], cwd: Path | None = None) -> None:
    """Run a command, log its output, and raise if it exits non-zero."""
    logger = get_run_logger()
    logger.info("Running: %s", " ".join(command))
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
    if result.stdout:
        logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"Command failed: {' '.join(command)}")


@task(retries=2, retry_delay_seconds=15)
def ingest_products() -> None:
    """Extract from the API and load into BigQuery. Retried on failure,
    since network calls are the most likely thing to fail transiently."""
    _run([sys.executable, str(INGEST_SCRIPT)])


@task
def dbt_run() -> None:
    """Build all dbt models."""
    _run(["dbt", "run"], cwd=ANALYTICS_DIR)


@task
def dbt_test() -> None:
    """Run all dbt data-quality tests."""
    _run(["dbt", "test"], cwd=ANALYTICS_DIR)


def notify_on_failure(flow, flow_run, state) -> None:
    """Fires when the flow fails. Plug a real alert channel in here."""
    logger = get_run_logger()
    logger.error("PIPELINE FAILED: run '%s' ended in state %s", flow_run.name, state.name)


@flow(name="ecommerce-elt-pipeline", log_prints=True, on_failure=[notify_on_failure])
def ecommerce_pipeline() -> None:
    """Ingest -> build -> test, in order. Tasks run sequentially, so a failure
    at any step stops the ones after it."""
    ingest_products()
    dbt_run()
    dbt_test()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        # Long-running process that triggers the pipeline on a schedule.
        ecommerce_pipeline.serve(name="daily", cron="0 6 * * *")
    else:
        # Run the whole pipeline once, immediately.
        ecommerce_pipeline()