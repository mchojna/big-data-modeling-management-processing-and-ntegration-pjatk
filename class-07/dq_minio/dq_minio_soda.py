import os
import io
import json
from datetime import datetime, timedelta, timezone
from dateutil import parser as dateparser

import boto3
import pandas as pd

# --- SODA imports (Python API) ---
from soda.scan import Scan

# ========== CONFIG ==========
MINIO_ENDPOINT_URL = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET = "datalake"

CUSTOMERS_KEY = "customers.csv"
ORDERS_KEY = "orders.csv"

LOCAL_DATA_DIR = "./_data_cache"
os.makedirs(LOCAL_DATA_DIR, exist_ok=True)


# ========== HELPERS ==========
def download_object_to_bytes(s3_client, bucket, key):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


def ensure_downloaded(s3_client, bucket, key, local_path):
    if not os.path.exists(local_path):
        print(f"Downloading s3://{bucket}/{key} -> {local_path}")
        data = download_object_to_bytes(s3_client, bucket, key)
        with open(local_path, "wb") as f:
            f.write(data)
    else:
        print(f"Using cached file: {local_path}")


# ========== MAIN ==========
def main():
    # 1) Connect to MinIO (S3-compatible)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT_URL,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    # 2) Download CSVs locally (students must have uploaded first)
    customers_path = os.path.join(LOCAL_DATA_DIR, "customers.csv")
    orders_path = os.path.join(LOCAL_DATA_DIR, "orders.csv")
    ensure_downloaded(s3, BUCKET, CUSTOMERS_KEY, customers_path)
    ensure_downloaded(s3, BUCKET, ORDERS_KEY, orders_path)

    # 3) Load CSVs into pandas
    customers = pd.read_csv(customers_path, dtype=str)
    orders = pd.read_csv(orders_path, dtype=str)

    # Cast types where appropriate
    # Keep raw columns as strings, then add typed views to avoid pandas NA headaches.
    def to_float(s):
        try:
            return float(s)
        except Exception:
            return None

    def to_int(s):
        try:
            return int(float(s))
        except Exception:
            return None

    def to_date(s):
        try:
            return dateparser.parse(s).date()
        except Exception:
            return None

    customers["_age"] = customers["age"].apply(to_int)
    customers["_created_at"] = customers["created_at"].apply(to_date)
    orders["_amount"] = orders["amount"].apply(to_float)
    orders["_order_date"] = orders["order_date"].apply(to_date)

    # 4) Build Soda Scan using pandas datasource
    scan = Scan()
    scan.set_verbose(True)
    # Declare we use the pandas data source
    scan.set_data_source_name("pandas")
    # Register dataframes (table-like names for checks to target)
    scan.add_pandas_dataframe(dataset_name="customers", pandas_df=customers)
    scan.add_pandas_dataframe(dataset_name="orders", pandas_df=orders)

    # 5) Soda checks (YAML as string). You can also keep these in .yml files.
    # The checks below cover: row counts, missing, duplicates, valid_format,
    # accepted values, numeric ranges, schema, reference integrity, and freshness.
    checks_yaml = r"""
checks for customers:
- row_count > 0
- schema:
    fail:
        when required column missing:
        - customer_id
        - full_name
        - email
        - country
        - age
        - created_at
- missing_count(customer_id) = 0
- duplicate_count(customer_id) = 0
- missing_percent(email) = 0
- invalid_percent(email) < 5%:
    valid_format: email
- values in country must be in:
    values: [PL, DE, FR]
- invalid_percent(age) = 0:
valid_format: number
- max(_age) <= 120
- min(_age) >= 18
- missing_percent(_age) < 5%
- freshness(created_at) < 90d

checks for orders:
- row_count > 0
- schema:
    fail:
        when required column missing:
        - order_id
        - customer_id
        - order_date
        - amount
        - status
- missing_count(order_id) = 0
- duplicate_count(order_id) = 0
- invalid_percent(amount) = 0:
    valid_format: number
- min(_amount) >= 0
- avg(_amount) >= 0
- values in status must be in:
    values: [pending, shipped, cancelled]
- freshness(order_date) < 60d
- values in customer_id must exist in customers.customer_id
"""

    # 6) Add the checks to the scan
    scan.add_sodacl_yaml_str(checks_yaml)
    # 7) Run scan and print results
    scan_result = scan.execute()
    # Pretty print outcome & failures
    print("\n=== SODA RESULT ===")
    print("Outcome:", scan.get_scan_results().get("scan", {}).get("outcome"))
    print(json.dumps(scan.get_scan_results(), indent=2))
    # Exit code hint for CI usage (0=pass, 1=fail)
    if scan.has_failures():
        print("\n❌ Data quality checks FAILED")
    else:
        print("\n✅ Data quality checks PASSED")


if __name__ == "__main__":
    main()
