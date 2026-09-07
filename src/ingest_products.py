"""Ingest the product catalog from the DummyJSON API into BigQuery.

Extract-and-load ("EL") step of the pipeline: pulls the full product catalog
from the public DummyJSON API, flattens each product into a row, and loads the
result into a raw BigQuery table. dbt then handles the transform ("T")
downstream into a product dimension.

Run with:
    uv run python src/ingest_products.py
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import requests
from google.cloud import bigquery

# --- Configuration -----------------------------------------------------------
PROJECT_ID = "dbt-bigquery-portfolio"   # <-- your GCP project ID
RAW_DATASET = "raw"                      # separate dataset for untransformed data
TABLE_NAME = "products"
LOCATION = "US"                          # must match your other datasets
# limit=0 returns the full catalog in one response (no pagination needed).
FEED_URL = "https://dummyjson.com/products?limit=0"
REQUEST_TIMEOUT_SECONDS = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
logger = logging.getLogger(__name__)


# --- Extract -----------------------------------------------------------------
def fetch_products(url: str) -> list[dict[str, Any]]:
    """Fetch the product catalog and return the list of product records."""
    logger.info("Fetching products from %s", url)
    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    products = response.json().get("products", [])
    logger.info("Received %d product records", len(products))
    return products


# --- Transform ---------------------------------------------------------------
def transform(products: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Select and flatten the fields we care about into rows for BigQuery."""
    ingested_at = datetime.now(tz=timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []

    for product in products:
        rows.append(
            {
                "product_id": product.get("id"),
                "title": product.get("title"),
                "category": product.get("category"),
                "brand": product.get("brand"),
                "price": product.get("price"),
                "discount_percentage": product.get("discountPercentage"),
                "rating": product.get("rating"),
                "stock": product.get("stock"),
                "sku": product.get("sku"),
                "weight": product.get("weight"),
                "availability_status": product.get("availabilityStatus"),
                "thumbnail": product.get("thumbnail"),
                "ingested_at": ingested_at,
            }
        )

    logger.info("Transformed %d rows", len(rows))
    return rows


# --- Load --------------------------------------------------------------------
def _table_schema() -> list[bigquery.SchemaField]:
    """Explicit schema so column types are controlled, not guessed."""
    return [
        bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("brand", "STRING"),
        bigquery.SchemaField("price", "FLOAT"),
        bigquery.SchemaField("discount_percentage", "FLOAT"),
        bigquery.SchemaField("rating", "FLOAT"),
        bigquery.SchemaField("stock", "INTEGER"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("weight", "FLOAT"),
        bigquery.SchemaField("availability_status", "STRING"),
        bigquery.SchemaField("thumbnail", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    ]


def load_to_bigquery(rows: list[dict[str, Any]]) -> None:
    """Create the raw dataset/table if needed and load the rows."""
    if not rows:
        logger.warning("No rows to load; skipping.")
        return

    client = bigquery.Client(project=PROJECT_ID)

    # Ensure the raw dataset exists (idempotent).
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{RAW_DATASET}")
    dataset_ref.location = LOCATION
    client.create_dataset(dataset_ref, exists_ok=True)

    table_id = f"{PROJECT_ID}.{RAW_DATASET}.{TABLE_NAME}"
    job_config = bigquery.LoadJobConfig(
        schema=_table_schema(),
        # Replace the table each run: a clean, idempotent snapshot with no
        # duplicates. (See the README note about moving to append + dedup
        # once you add incremental models.)
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    logger.info("Loading %d rows into %s", len(rows), table_id)
    load_job = client.load_table_from_json(rows, table_id, job_config=job_config)
    load_job.result()  # wait for completion

    table = client.get_table(table_id)
    logger.info("Load complete: %d total rows in %s", table.num_rows, table_id)


# --- Orchestrate -------------------------------------------------------------
def main() -> None:
    products = fetch_products(FEED_URL)
    rows = transform(products)
    load_to_bigquery(rows)
    logger.info("Ingestion finished successfully.")


if __name__ == "__main__":
    main()