# REDCap ETL with REDCap API Extraction

This project runs an Apache Airflow DAG that:

1. Extracts records from REDCap using the REDCap API.
2. Transforms the extracted dataset into SQL-safe column names.
3. Loads the results into Postgres table `public.redcap_data`.

The pipeline is implemented in `dags/redcapetl.py` and scheduled to run daily.

## What the DAG Does

The `redcap_etl` DAG includes two tasks:

1. `extract_data(api_url, api_key)`
   - Connects to REDCap using PyCap (`redcap.Project`).
   - Calls `export_records(format_type='csv', raw_or_label='raw')`.
   - Returns raw CSV text.

2. `load_data_to_postgres(transformed_redcap_data)`
   - Parses CSV to a pandas DataFrame.
   - Normalizes column names to lowercase alphanumeric/underscore format.
   - Drops and recreates `public.redcap_data` with all columns as `TEXT`.
   - Bulk inserts rows into Postgres using `psycopg2.extras.execute_values`.

## Project Files

- `dags/redcapetl.py`: Main Airflow DAG for REDCap extraction and Postgres loading.
- `docker-compose.yml`: Local Postgres service.
- `requirements.txt`: Python dependencies (PyCap, pandas, psycopg2, etc.).
- `Dockerfile`: Astro Runtime image configuration.
- `.env`: Local environment values for runtime configuration.

## Prerequisites

- Docker Desktop (or compatible Docker engine)
- Astro CLI
- REDCap API URL and API key/token for your project

## Configuration

Set these environment variables so the DAG can authenticate to REDCap:

- `API_URL`: REDCap API endpoint URL
- `API_KEY`: REDCap API token

This DAG also uses Airflow Postgres connection id `postgres_default`.

For local development, Postgres is provided by `docker-compose.yml` with:

- host: `localhost`
- port: `5432`
- database: `postgres`
- username: `postgres`
- password: `postgres`

Ensure `postgres_default` points to those values (or update the DAG/connection accordingly).

## Run Locally

1. Start local Postgres dependency:

   ```powershell
   docker compose up -d postgres
   ```

2. Start Airflow with Astro:

   ```powershell
   astro dev start
   ```

3. Open the Airflow UI from Astro output, then trigger DAG `redcap_etl`.

## Data Load Behavior

- Table is replaced on each run:
  - `DROP TABLE IF EXISTS public.redcap_data`
  - `CREATE TABLE public.redcap_data (...)`
- All columns are loaded as `TEXT`.
- Missing values are inserted as SQL `NULL`.

If you want append behavior or typed columns, adjust `load_data_to_postgres` in `dags/redcapetl.py`.

## Validation

After a successful run, validate data in Postgres:

```sql
SELECT COUNT(*) FROM public.redcap_data;
SELECT * FROM public.redcap_data LIMIT 20;
```

## Troubleshooting

- Empty extraction result:
  - Confirm `API_URL` and `API_KEY` are set correctly.
  - Confirm the REDCap token has export permissions.
- Postgres connection failures:
  - Confirm Postgres container is running.
  - Confirm `postgres_default` settings match local container values.
- Schema mismatches downstream:
  - Column names are normalized during load, so update downstream queries accordingly.
