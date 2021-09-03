from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from time import sleep
from datetime import datetime


''' Defines when DAG starts'''

default_args={

'start_date':datetime(2021, 9, 1),
 'catchup':False,
 'schedule_interval':'@daily',
 'description':'Third DAG'

}


def print_hello():
	sleep(5)
	return 'Hello World'


with DAG('hello_world_default_args_2',  default_args=default_args) as dag:

	dummy_task 	= DummyOperator(task_id='dummy_task', retries=3)
    
	python_task	= PythonOperator(task_id='python_task', python_callable=print_hello)

    # Operator order

	dummy_task >> python_task


