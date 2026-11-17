import os
import psycopg2
import pandas as pd
from google.cloud import bigquery

# ============================================================
# KONFIGURACJA -- sekwencyjne ładowanie wg relacji PK -> FK
# ============================================================

NEON_CONNECTION_STRING = os.environ["NEON_CONNECTION_STRING"]
BQ_PROJECT_ID = os.environ["BQ_PROJECT_ID"]
BQ_DATASET = os.environ["BQ_DATASET"]

# Kolejność tabel zgodna z hierarchią kluczy głównych i obcych (PK -> FK)
TABLES_IN_ORDER = [
    "personel_medyczny",
    "komorki",
    "pacjenci",
    "zabiegi",
    "leki",
    "pobyty",
    "leki_na_pobyt",
    "zabiegi_na_pobyt",
]

INCREMENTAL_COLUMN = "updated_at"

# Na start ustawiamy "full_refresh", aby załadować wszystkie 8 tabel;
# następnie zmienimy na "incremental" do testu dodawania i edycji rekordów.
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


def extract_from_neon(table_name: str, mode: str, last_ts=None) -> pd.DataFrame:
    conn = psycopg2.connect(NEON_CONNECTION_STRING)
    try:
        if mode == "incremental" and last_ts is not None:
            query = f"SELECT * FROM {table_name} WHERE {INCREMENTAL_COLUMN} > %s"
            df = pd.read_sql(query, conn, params=(last_ts,))
        else:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)
    finally:
        conn.close()
    return df


def load_to_bigquery(bq_client: bigquery.Client, df: pd.DataFrame, table_ref: str, mode: str):
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
    bq_client = bigquery.Client()

    for table_name in TABLES_IN_ORDER:
        print(f"--- Przetwarzanie tabeli: {table_name} ---")
        table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}"

        last_ts = None
        if LOAD_MODE == "incremental":
            last_ts = get_last_loaded_timestamp(bq_client, table_ref)
            print(f"[{table_name}] Ostatni załadowany timestamp w BigQuery: {last_ts}")

        df = extract_from_neon(table_name, LOAD_MODE, last_ts)
        print(f"[{table_name}] Wyciągnięto {len(df)} wierszy z Neon")

        if df.empty:
            print(f"[{table_name}] Brak nowych danych -- pomijam.")
            continue

        job = load_to_bigquery(bq_client, df, table_ref, LOAD_MODE)
        print(f"[{table_name}] Załadowano {job.output_rows} wierszy do {table_ref} (tryb: {LOAD_MODE})")


if __name__ == "__main__":
    main()
