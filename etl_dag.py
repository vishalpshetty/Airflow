#step 1
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators import BashOperator
from airflow.operators.python_operator import PythonOperator

from etl import run_etl
#from create_tables import *

#step 2
default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2021, 1, 31),
    'depends_on_past': False,
    'retries': 0
}

#step 3
dag = DAG('ETL1_bash',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          catchup=False,
          schedule_interval='@once')

#step 4
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


also_run_this = PythonOperator(
    task_id='load_redshift',
    python_callable=run_etl,
    dag=dag,
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#step 5
start_operator >> also_run_this
also_run_this >> end_operator
