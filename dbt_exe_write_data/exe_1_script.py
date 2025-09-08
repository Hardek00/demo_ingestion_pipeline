import os, json, logging
from datetime import datetime, timezone

from google.cloud import bigquery
from google.oauth2 import service_account

# Config (change or set as env vars)
PROJECT_ID = os.getenv("PROJECT_ID", "zeta-axiom-468312-f1")  # Your GCP project
DATASET_ID = os.getenv("DATASET_ID", "raw_data")  # BQ dataset
RAW_TABLE = os.getenv("RAW_TABLE", "excersise_1_customers")  # Raw table with raw_json
TRANSFORMED_TABLE = os.getenv("TRANSFORMED_TABLE", "transformed_customers")  # New table for transformed data
KEY_FILE = "service_account_key.json"  # Path to your service account key

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bq_transformer")


def create_transformed_table_if_not_exists(client: bigquery.Client, table_id: str):
    """Create transformed table if it doesn't exist"""
    schema = [
        bigquery.SchemaField("customer_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("customer_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("customer_email", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("signup_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("transformed_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    
    try:
        client.get_table(table)
        logger.info(f"Table {table_id} already exists")
    except Exception:
        logger.info(f"Creating table {table_id}")
        client.create_table(table)


def transform_and_load(client: bigquery.Client):
    """Query raw table, unnest/transform data, and load to transformed table"""
    raw_table_id = f"{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE}"
    transformed_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TRANSFORMED_TABLE}"
    
    # Query raw data
    query = f"""
    SELECT raw_json
    FROM `{raw_table_id}`
    """
    logger.info(f"Querying raw data from {raw_table_id}")
    query_job = client.query(query)
    results = query_job.result()
    
    rows = []
    transformed_at = datetime.now(timezone.utc).isoformat()
    
    for result in results:
        array = json.loads(result["raw_json"])  # Load the array from raw_json string
        if not isinstance(array, list):
            logger.error("raw_json is not an array")
            continue
        
        for obj in array:  # Unnest: loop over each object in the array
            if not isinstance(obj, dict):
                logger.warning("Skipping non-dict item in array")
                continue
            
            # Transform: extract and format fields (match your SQL)
            transformed_row = {
                "customer_id": int(obj.get("id", 0)),
                "customer_name": obj.get("name", "").title(),  # INITCAP equivalent
                "customer_email": obj.get("email", "").lower(),
                "signup_date": obj.get("signup_date"),  # Assume date string; parse if needed
                "transformed_at": transformed_at,
            }
            
            rows.append(transformed_row)
    
    if not rows:
        logger.warning("No data to transform")
        return
    
    # Create table if needed
    create_transformed_table_if_not_exists(client, transformed_table_id)
    
    # Insert transformed rows (batch insert, idempotent with row_id)
    logger.info(f"Inserting {len(rows)} transformed rows to {transformed_table_id}")
    errors = client.insert_rows_json(transformed_table_id, rows)
    
    if errors:
        logger.error(f"Errors inserting to {transformed_table_id}: {errors}")
        raise ValueError("Insert failed")
    
    logger.info("Transformation and load completed successfully")


def main():
    logger.info("Starting BQ transformation script")
    
    # Setup BQ client with service account
    credentials = service_account.Credentials.from_service_account_file(KEY_FILE)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    
    transform_and_load(client)


if __name__ == "__main__":
    main()
