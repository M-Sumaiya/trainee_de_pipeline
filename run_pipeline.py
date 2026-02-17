import os
import sys
import logging
import pandas as pd
from google.cloud import bigquery
from pipeline.extract import extract_csv
from pipeline.transform import (
    transform_sales,
    transform_financial,
    transform_attendance
)
from pipeline.load import load_dataframe


# CONFIGURATION
# -----------------------------
PROJECT_ID = "trainee-data-engineering"
DATASET = "trainee_de_dataset"

TABLE_SALES = f"{PROJECT_ID}.{DATASET}.sales"
TABLE_FINANCIAL = f"{PROJECT_ID}.{DATASET}.financial"
TABLE_ATTENDANCE = f"{PROJECT_ID}.{DATASET}.attendance"

# CSV file paths
CSV_SALES = "data/sales_dataset_3m.csv"
CSV_FINANCIAL = "data/financial_dataset_3m.csv"
CSV_ATTENDANCE = "data/attendance_dataset_3m.csv"


# LOGGING SETUP
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# HELPER FUNCTIONS
# -----------------------------
def check_file(file_path): 
    if not os.path.exists(file_path): #Exit if the file does not exist
        logger.error(f"File not found: {file_path}")
        sys.exit(1)

#    Load a dataset into BigQuery in an idempotent way.First chunk: WRITE_TRUNCATE, subsequent chunks: WRITE_APPEND
def load_dataset(csv_path, transform_func, table_id, client, dataset_name):
    logger.info(f"Loading {dataset_name} dataset...")
    try:
        for i, chunk in enumerate(extract_csv(csv_path), start=1):
            df = transform_func(chunk)
            disposition = "WRITE_TRUNCATE" if i == 1 else "WRITE_APPEND"
            load_dataframe(df, table_id, client, write_disposition=disposition)
            logger.info(f"Chunk {i} loaded into {table_id}")
        logger.info(f"{dataset_name} dataset loaded successfully.\n")
    except Exception as e:
        logger.error(f"Error loading {dataset_name} dataset: {e}")


# MAIN PIPELINE
# -----------------------------
def main():
    # 1. Check CSVs exist
    for file in [CSV_SALES, CSV_FINANCIAL, CSV_ATTENDANCE]:
        check_file(file)

    # 2. Initialize BigQuery client
    try:
        client = bigquery.Client()
        logger.info("BigQuery client initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        sys.exit(1)

    # 3. Load datasets
    load_dataset(CSV_SALES, transform_sales, TABLE_SALES, client, "SALES")
    load_dataset(CSV_FINANCIAL, transform_financial, TABLE_FINANCIAL, client, "FINANCIAL")
    load_dataset(CSV_ATTENDANCE, transform_attendance, TABLE_ATTENDANCE, client, "ATTENDANCE")

    logger.info("All datasets processed successfully!")


# ENTRY POINT
# -----------------------------
if __name__ == "__main__":
    main()
