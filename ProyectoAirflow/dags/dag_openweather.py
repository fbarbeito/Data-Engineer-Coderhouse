from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from scripts.main import exec_etl_staging
from Readme.readme import dag_principal

default_args={
    'owner': 'Fernando Barbeito',
    'retries': 2,
    'retry_delay': timedelta(minutes=1) # Reintento cada 3 minutos si falla
}



with DAG(
    dag_id="extract_data",
    default_args= default_args,
    doc_md = dag_principal,
    start_date=datetime.today(),
    schedule_interval="0 * * * *" ) as dag:

    # task con dummy operator
    dummy_start_task = DummyOperator(
        task_id="start"
    )

    create_tables_task = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="redfshit_conn",
        sql="queries/create_tables.sql",
        params={	
            "schema": "barbeito26_coderhouse"
        }
    )
    
    load_weather_data_task = PythonOperator(
        task_id="extract_weather",
        python_callable=exec_etl_staging,
        op_kwargs={
            "path_cred": "/opt/airflow/config/config.ini",
            "path_beach": "/opt/airflow/config/balneariosUy.json"
        }
    )

    dim_tables_task = PostgresOperator(
        task_id="dim_tables",
        postgres_conn_id="redfshit_conn",
        sql="queries/dim_tables.sql",
        params={	
            "schema": "barbeito26_coderhouse"
        }
    )

    fact_table_task = PostgresOperator(
        task_id="fact_table",
        postgres_conn_id="redfshit_conn",
        sql="queries/fact_table.sql",
        params={	
            "schema": "barbeito26_coderhouse"
        }
    )

    dummy_end_task = DummyOperator(
        task_id="end"
    )

    dummy_start_task >> create_tables_task
    create_tables_task >> load_weather_data_task
    load_weather_data_task >> dim_tables_task
    dim_tables_task >> fact_table_task
    fact_table_task >> dummy_end_task