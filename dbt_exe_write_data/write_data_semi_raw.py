import os, json, logging
from datetime import datetime, timezone

from google.cloud import bigquery
from google.oauth2 import service_account

# Config (change or set as env vars)
PROJECT_ID = os.getenv("PROJECT_ID", "zeta-axiom-468312-f1")  # Your GCP project
DATASET_ID = os.getenv("DATASET_ID", "raw_data")  # BQ dataset
KEY_FILE = "service_account_key.json"  # Path to your service account key
# Only this file should be loaded for this exercise
FILE_PATH = os.getenv("FILE_PATH", os.path.join("data", "customers.json"))
# Single, fixed target table
TABLE_NAME = os.getenv("TABLE_NAME", "excersise_2_customers")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bq_semi_raw_loader")

def ensure_dataset(client: bigquery.Client, project_id: str, dataset_id: str):
    ref = bigquery.DatasetReference(project_id, dataset_id)
    try:
        client.get_dataset(ref)
    except Exception:
        client.create_dataset(bigquery.Dataset(ref))

def create_table_if_not_exists(client: bigquery.Client, table_id: str, schema: list):
    """Create BQ table if it doesn't exist with given schema"""
    table = bigquery.Table(table_id, schema=schema)
    try:
        client.get_table(table)
        logger.info(f"Table {table_id} already exists")
    except Exception:
        logger.info(f"Creating table {table_id}")
        client.create_table(table)


def load_customers_to_bq(file_path: str):
    """Load customers.json as structured rows into a single BQ table"""
    # Load JSON data (expect array of objects)
    with open(file_path, "r") as f:
        array = json.load(f)

    if not isinstance(array, list):
        logger.error(f"{file_path} is not a JSON array")
        return

    # Prepare rows
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
    loaded_at = datetime.now(timezone.utc).isoformat()
    rows = []

    for obj in array:
        if not isinstance(obj, dict):
            logger.warning("Skipping non-dict item")
            continue
        row = {
            "customer_id": int(obj.get("id", 0)),
            "name": obj.get("name", ""),
            "email": obj.get("email", ""),
            "signup_date": obj.get("signup_date", ""),  # Keep as string for simplicity
            "loaded_at": loaded_at,
        }
        rows.append(row)

    if not rows:
        logger.warning(f"No rows to load from {file_path}")
        return

    # Schema (all NULLABLE for simplicity in the exercise)
    schema = [
        bigquery.SchemaField("customer_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("signup_date", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="NULLABLE"),
    ]

    # Setup BQ client
    credentials = service_account.Credentials.from_service_account_file(KEY_FILE)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

    # Ensure dataset exists, then create table if needed
    ensure_dataset(client, PROJECT_ID, DATASET_ID)
    create_table_if_not_exists(client, table_id, schema)

    # Insert rows
    logger.info(f"Inserting {len(rows)} rows to {table_id}")
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        logger.error(f"Errors inserting to {table_id}: {errors}")
        raise ValueError("Insert failed")

    logger.info(f"Successfully loaded {file_path} to {table_id}")


def main():
    logger.info("Starting BQ semi-raw data loader (customers only)")
    if not os.path.exists(FILE_PATH):
        logger.error(f"File not found: {FILE_PATH}")
        return
    load_customers_to_bq(FILE_PATH)


if __name__ == "__main__":
    main()
