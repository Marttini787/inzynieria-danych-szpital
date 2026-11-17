import os
import dlt
from dlt.sources.sql_database import sql_database

# Konfiguracja zgodna z wymaganiami zadania (pkt 11 i 15)
source_table = "pacjenci"
incremental_column = "updated_at"
load_mode = "full"  # 'full' dla pełnego odświeżenia, 'incremental' dla przyrostowego


def run_pipeline():
    db_url = os.environ["DATABASE_URL"]

    source = sql_database(
        credentials=db_url,
        table_names=[source_table],
    )

    if load_mode == "incremental":
        # Śledzenie zmian po kolumnie updated_at (CDC)
        getattr(source, source_table).apply_hints(
            incremental=dlt.sources.incremental(incremental_column)
        )
        write_disposition = "merge"
    else:
        write_disposition = "replace"

    pipeline = dlt.pipeline(
        pipeline_name="neon_to_bigquery",
        destination="bigquery",
        dataset_name=os.environ.get("BQ_DATASET", "raw_data"),
    )

    load_info = pipeline.run(
        source,
        write_disposition=write_disposition,
        primary_key="id",
    )
    print(load_info)


if __name__ == "__main__":
    run_pipeline()
