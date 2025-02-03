# two task dag
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Jay',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
    'start_date': datetime(2025, 1, 22)
}

with DAG(
    dag_id='two_task_dag',
    description = 'have two bash operator dag',
    schedule_interval = None,
    default_args=default_args
) as dag:
    
    task1 = BashOperator(
            task_id='first_task',
            bash_command='echo "First Airflow task completed"')
    
    task2 = BashOperator(
            task_id='second_task',
            bash_command='echo "Sleeping..." && echo "Second Airflow task"')
    
    task1 >> task2