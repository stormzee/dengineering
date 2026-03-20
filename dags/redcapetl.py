from io import StringIO
from airflow import DAG
from airflow.providers.https.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import pandas as pd
import requests


POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'redcap_api'
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}


endpoint = {
    'token': 'B0DD2F63B1B7CD51B01B0D17F80C16F9',
    'content': 'record',
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'csvDelimiter': '',
    'rawOrLabel': 'raw',
    'rawOrLabelHeaders': 'raw',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}
# r = requests.post('http://192.168.10.83/redcap/api/', data=data)


with DAG(dag_id='redcap_etl',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    @task()
    def extract_csv_data():

        http_hook = HttpHook(method='GET', http_hook_id=API_CONN_ID)
        res = http_hook.run(endpoint=endpoint)

        try:
            if res.status_code == 200:
                return res.text
        except requests.RequestException as e:
            print(f"Error occurred: {e}")


    @task()
    def transform_redcap_data(response):
        df = pd.read_csv(StringIO(response))
        return df.to_csv(index=False)
    

    @task()
    def load_data_to_postgres(csv_data):
        pg_hook = PostgresHook(POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()

        csv_data.to_sql('redcap_data', con=conn, if_exists='replace', index=False)


# create etl workflow

redacp_data = extract_csv_data()
transform_redcap_data = transform_redcap_data(redacp_data)
load_data_to_postgres(transform_redcap_data)
        
