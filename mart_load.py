from airflow import DAG
from datetime import datetime, timedelta 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
 

LOAD_MART = "select shops.f_mart('202103')"

DB_CONN = "gp_conn"
 


default_args = {
    'depends_on_past': False,
    'owner': 'usr',
    'start_date': datetime(2024,1,16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    "mart_load_dag",
    max_active_runs=3,
    schedule_interval='59 23 * * *',
    default_args=default_args,
    catchup=False,
) as dag:
    
    task_start = DummyOperator(task_id="start")
    
    task_part = PostgresOperator(task_id="start_load_mart",
                                postgres_conn_id=DB_CONN,
                                sql=LOAD_MART
                                )
     
    task_end = DummyOperator(task_id="end")
    
    
    task_start>>task_part>>task_end
