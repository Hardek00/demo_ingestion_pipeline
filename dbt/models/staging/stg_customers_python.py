# models/staging/stg_customers_python.py
# Pure Python/pandas staging of packed JSON -> one row per customer.

import json
import pandas as pd

def model(dbt, session):
    dbt.config(materialized="table", submission_method="bigframes")

    # Get the source relation and read it as a DataFrame
    src_rel = dbt.source("raw", "excersise_1_customers")  # no Jinja here
    pdf = session.table(str(src_rel)).to_pandas()         # expects column 'raw_json'

    # Helper to normalize each raw_json cell to a list of dicts
    def parse_to_list(val):
        if pd.isna(val):
            return []
        data = val
        if isinstance(val, str):
            try:
                data = json.loads(val)
            except Exception:
                return []
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            return [data]
        return []

    # Explode all rows
    records = []
    if not pdf.empty:
        for _, row in pdf.iterrows():
            for obj in parse_to_list(row.get("raw_json")):
                records.append(obj)

    exploded = pd.DataFrame(records)

    # If nothing parsed, return empty schema
    if exploded.empty:
        empty = pd.DataFrame(
            columns=["customer_id", "customer_name", "customer_email", "signup_date"]
        )
        return session.create_dataframe(empty)

    # Clean & type
    out = pd.DataFrame()
    out["customer_id"] = pd.to_numeric(exploded.get("id"), errors="coerce").astype("Int64")
    out["customer_name"] = exploded.get("name").astype("string").str.title()
    out["customer_email"] = exploded.get("email").astype("string").str.lower()
    out["signup_date"] = pd.to_datetime(exploded.get("signup_date"), errors="coerce").dt.date

    out = out.dropna(subset=["customer_id"]).copy()
    out["customer_id"] = out["customer_id"].astype("int64")

    return session.create_dataframe(out)
