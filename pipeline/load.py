from google.cloud import bigquery

# Load DataFrame into BigQuery using a staging table and MERGE for idempotency.
def load_dataframe(df, table_id, client, unique_key, staging_suffix="_staging"):
    staging_table_id = f"{table_id}{staging_suffix}"

    # Load data into staging table (overwrite)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df, staging_table_id, job_config=job_config).result()
    print(f"Loaded {len(df)} rows into staging table {staging_table_id}")

    # Merge into main table
    merge_sql = f"""
    MERGE `{table_id}` T
    USING `{staging_table_id}` S
    ON T.{unique_key} = S.{unique_key}
    WHEN NOT MATCHED THEN
      INSERT ({', '.join(df.columns)})
      VALUES ({', '.join('S.'+c for c in df.columns)});
    """
    client.query(merge_sql).result()
    print(f"Merged staging table into main table {table_id}")

    # Delete staging table
    client.delete_table(staging_table_id, not_found_ok=True)
