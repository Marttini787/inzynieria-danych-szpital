import os
import psycopg2
import pandas as pd
from google.cloud import bigquery

# ============================================================
# KONFIGURACJA -- tu wprowadzacie zmiany na labie
# ============================================================

NEON_CONNECTION_STRING = os.environ["NEON_CONNECTION_STRING"]
BQ_PROJECT_ID = os.environ["BQ_PROJECT_ID"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_TABLE = os.environ["BQ_TABLE"]

SOURCE_TABLE = "pacjenci"
INCREMENTAL_COLUMN = "updated_at"
LOAD_MODE = "incremental"

# ============================================================
# LOGIKA
# ============================================================

def get_last_loaded_timestamp(bq_client: bigquery.Client, table_ref: str):
    query = f"SELECT MAX({INCREMENTAL_COLUMN}) AS max_ts FROM `{table_ref}`"
    try:
        result = bq_client.query(query).result()
        row = next(iter(result))
        return row.max_ts
    except Exception:
        return None

def extract_from_neon(mode: str, last_ts=None) -> pd.DataFrame:
    conn = psycopg2.connect(NEON_CONNECTION_STRING)
    try:
        if mode == "incremental" and last_ts is not None:
            query = f"SELECT * FROM {SOURCE_TABLE} WHERE {INCREMENTAL_COLUMN} > %s"
            df = pd.read_sql(query, conn, params=(last_ts,))
        else:
            query = f"SELECT * FROM {SOURCE_TABLE}"
            df = pd.read_sql(query, conn)
    finally:
        conn.close()
    return df

def load_to_bigquery(df: pd.DataFrame, table_ref: str, mode: str):
    bq_client = bigquery.Client()
    write_disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if mode == "full_refresh"
        else bigquery.WriteDisposition.WRITE_APPEND
    )
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    return job

def main():
    table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    bq_client = bigquery.Client()

    last_ts = None
    if LOAD_MODE == "incremental":
        last_ts = get_last_loaded_timestamp(bq_client, table_ref)
        print(f"Ostatni załadowany timestamp w BigQuery: {last_ts}")

    df = extract_from_neon(LOAD_MODE, last_ts)
    print(f"Wyciągnięto {len(df)} wierszy z Neon (tabela źródłowa: {SOURCE_TABLE})")

    if df.empty:
        print("Brak nowych danych do załadowania -- kończę.")
        return

    job = load_to_bigquery(df, table_ref, LOAD_MODE)
    print(f"Załadowano {job.output_rows} wierszy do {table_ref} (tryb: {LOAD_MODE})")

if __name__ == "__main__":
    main()
