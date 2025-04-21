from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

 
DB_CONN = "gp_conn"

DB_PROC_LOAD = 'f_full_load'

DB_SCHEMA = 'shops'
 
FULL_LOAD_TABLES = ['region','chanel','product','price']

FULL_LOAD_QUERY = f"select {DB_SCHEMA}.{DB_PROC_LOAD}(%(table_name)s);"

default_args = {
    'depends_on_past': False,
    'owner': 'usr',
    'start_date': datetime(2024,1,16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    "full_load_dag",
    max_active_runs=3,
    schedule_interval='50 23 * * *',
    default_args=default_args,
    catchup=False,
) as dag:
    
    task_start = DummyOperator(task_id="start")
    
    
    with TaskGroup("data_load") as task_data_load:
        for table in FULL_LOAD_TABLES: 
            task = PostgresOperator(task_id=f"data_load{table}",
                                postgres_conn_id=DB_CONN,
                                sql=FULL_LOAD_QUERY,
                                parameters={'table_name':f'{table}'}
                                )
    task_end = DummyOperator(task_id="end")
    
    
    task_start>>task_data_load>>task_end