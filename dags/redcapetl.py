from io import StringIO

from numpy import extract
from airflow.sdk import dag, Asset, Param, task, chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from redcap import Project
from psycopg2.extras import execute_values
import logging
import os
from pendulum import datetime, duration

# Optional: Setup basic logging to track errors
t_log = logging.getLogger("airflow.task")

API_URL = os.getenv("API_URL")  
API_KEY = os.getenv("API_KEY")  # Replace with your actual API key
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'redcap_api'


@dag(
    start_date=datetime(2026, 7, 1),  # date after which the DAG can be scheduled
    schedule="@daily",  # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    max_consecutive_failed_dag_runs=5,  # auto-pauses the DAG after 5 consecutive failed runs, experimental
    max_active_runs=1,  # only allow one concurrent run of this DAG, prevents parallel DuckDB calls
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args={
        "owner": "Sam",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": duration(seconds=30),  # tasks wait 30s in between retries
    },  # default_args are applied to all tasks in a DAG
    tags=["redcap", "ETL"],  # add tags in the UI
    is_paused_upon_creation=False, # start running the DAG as soon as its created
)
def redcap_etl():
    """ETL DAG to extract data from REDCap, 
    transform it, 
    and load it into a Postgres database."""

    @task()
    def extract_data(api_url, api_key):
        """
        Fetches data from REDCap and saves it as a CSV file.
        Returns the DataFrame if successful, otherwise returns None.
        """
        try:
            project = Project(api_url, api_key)
            data = project.export_records(format_type='csv', raw_or_label='raw')

            if not data or len(data.strip()) == 0:
                print("Warning: No data was returned from the REDCap export.")
                return None

            t_log.info(f"Success: Data extracted successfully")
            return data  # Return the raw CSV data for further processing

        except Exception as e:
            t_log.error(f"Failed to fetch REDCap data: {e}")
            return None
    

    @task()
    def load_data_to_postgres(transformed_redcap_data: str | None):
        if not transformed_redcap_data:
            raise ValueError("No REDCap data received from extract task.")

        pg_hook = PostgresHook(POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        df = pd.read_csv(StringIO(transformed_redcap_data))
        t_log.info(f"DataFrame shape: {df.shape}")

        # Keep identifiers simple/safe for SQL by normalizing column names.
        normalized_columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(r"[^a-zA-Z0-9_]", "_", regex=True)
            .str.replace(r"_+", "_", regex=True)
            .str.strip("_")
        )
        df.columns = [col if col else "col" for col in normalized_columns]

        create_columns = ", ".join([f'"{col}" TEXT' for col in df.columns])
        insert_columns = ", ".join([f'"{col}"' for col in df.columns])

        cursor.execute("DROP TABLE IF EXISTS public.redcap_data")
        cursor.execute(f"CREATE TABLE public.redcap_data ({create_columns})")

        rows = df.where(pd.notna(df), None).astype(object).values.tolist()
        if rows:
            execute_values(
                cursor,
                f"INSERT INTO public.redcap_data ({insert_columns}) VALUES %s",
                rows,
                page_size=1000,
            )

        conn.commit()
        cursor.close()
        conn.close()
        t_log.info("Data loaded into Postgres successfully.")

# create etl workflow
    extracted = extract_data(API_URL, API_KEY)
    load = load_data_to_postgres(extracted)
        
    chain(
        extracted,
        load
    )


# instantiate the DAG
redcap_etl()
