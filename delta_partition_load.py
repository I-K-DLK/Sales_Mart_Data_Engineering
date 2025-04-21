from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
 
DB_CONN = "gp_conn"

DB_PROC_LOAD = 'f_load_delta_partition'

DB_SCHEMA = 'shops'
 
TABLES = ['sales','plan']



PARTITION_KEY = 'date'

START_DATE = '2021-03-01'

END_DATE = '2021-04-01'

DELTA_PARTITION_QUERY = f"select {DB_SCHEMA}.{DB_PROC_LOAD}(%(table)s,%(external_table)s,%(partition_key)s,%(start_date)s,%(end_date)s);"

default_args = {
    'depends_on_past': False,
    'owner': 'usr',
    'start_date': datetime(2024,1,16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    "delta_partition_dag",
    max_active_runs=3,
    schedule_interval='50 23 * * *',
    default_args=default_args,
    catchup=False,
) as dag:
    
    task_start = DummyOperator(task_id="start")
    
    
    with TaskGroup("data_load") as task_data_load:
        for table in TABLES:
            task = PostgresOperator(task_id=f"data_load{table}",
                                postgres_conn_id =DB_CONN,
                                sql=DELTA_PARTITION_QUERY,
                                parameters={'table':f'{table}','external_table':f'''{table+'_ext'}''',
                                            'partition_key':f'{PARTITION_KEY}','start_date':f'{START_DATE}',
                                            'end_date':f'{END_DATE}'}
                                )
    task_end = DummyOperator(task_id="end")
    
    
    task_start>>task_data_load>>task_end
