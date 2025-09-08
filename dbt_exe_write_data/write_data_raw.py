import os, json, logging
from datetime import datetime, timezone
from glob import glob

from google.cloud import bigquery
from google.oauth2 import service_account

# Config (change or set as env vars)
PROJECT_ID = os.getenv("PROJECT_ID", "zeta-axiom-468312-f1")  # Your GCP project
DATASET_ID = os.getenv("DATASET_ID", "raw_data")  # BQ dataset to use/create tables in
KEY_FILE = "service_account_key.json"  # Path to your service account key
DATA_FOLDER = "data"  # Folder with JSON files

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bq_loader")


def create_table_if_not_exists(client: bigquery.Client, table_id: str):
    """Create BQ table if it doesn't exist (simple schema for raw data)"""
    schema = [
        bigquery.SchemaField("raw_json", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    try:
        client.get_table(table)
        logger.info(f"Table {table_id} already exists")
    except Exception:
        logger.info(f"Creating table {table_id}")
        client.create_table(table)


def load_json_to_bq(file_path: str):
    """Load a single JSON file to BQ as raw data"""
    # Load JSON data
    with open(file_path, "r") as f:
        data = json.load(f)
    
    # Prepare row
    base_name = os.path.basename(file_path).replace(".json", "")
    table_id = f"{PROJECT_ID}.{DATASET_ID}.excersise_1_{base_name}"
    fetched_at = datetime.now(timezone.utc).isoformat()
    row = {
        "raw_json": json.dumps(data, ensure_ascii=False),
        "source": file_path,  # or just base_name if preferred
        "fetched_at": fetched_at,
    }
    
    # Setup BQ client with service account
    credentials = service_account.Credentials.from_service_account_file(KEY_FILE)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    
    # Create table if needed
    create_table_if_not_exists(client, table_id)
    
    # Insert row (idempotent with row_id)
    row_id = f"{file_path}:{fetched_at}"
    logger.info(f"Inserting row to {table_id}")
    errors = client.insert_rows_json(table_id, [row], row_ids=[row_id])
    
    if errors:
        logger.error(f"Errors inserting to {table_id}: {errors}")
        raise ValueError("Insert failed")
    
    logger.info(f"Successfully loaded {file_path} to {table_id}")


def main():
    logger.info("Starting BQ raw data loader")
    
    # Find all JSON files in data/
    json_files = glob(os.path.join(DATA_FOLDER, "*.json"))
    if not json_files:
        logger.error(f"No JSON files found in {DATA_FOLDER}")
        return
    
    for file_path in json_files:
        try:
            load_json_to_bq(file_path)
        except Exception as e:
            logger.error(f"Failed to process {file_path}: {e}")


if __name__ == "__main__":
    main()
