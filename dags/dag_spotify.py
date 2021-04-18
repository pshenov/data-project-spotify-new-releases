import datetime as dt

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from etl_spotify import run_etl_spotify

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2021, 4, 16),
}


with DAG(dag_id='etl_spotify',
         schedule_interval='@daily',
         default_args=default_args) as dag:

    run_etl = PythonOperator(
        task_id='run_etl_spotify',
        python_callable=run_etl_spotify,
        dag=dag,
    )