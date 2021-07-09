from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from etl_spotify import get_tracks, save_tracks_to_db

with DAG(dag_id='etl_spotify',
         start_date=datetime(2021, 7, 1),
         schedule_interval='@daily'
) as dag:


    create_tracks_table = PostgresOperator(
        task_id='create_table',
        sql='create_table.sql',
        postgres_conn_id='postgres_default',
    )

    getting_spotify_data = PythonOperator(
        task_id='getting_data',
        python_callable=get_tracks,
        provide_context=True,
    )

    insert_data_to_db = PythonOperator(
        task_id='insert_data_to_db',
        python_callable=save_tracks_to_db,
        provide_context=True
    )

    create_tracks_table >> getting_spotify_data >> insert_data_to_db
