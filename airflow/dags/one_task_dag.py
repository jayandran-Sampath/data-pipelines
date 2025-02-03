'''One Task DAG'''
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow import DAG

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
        dag_id='one_task_dag',
        description='A one task Airflow DAG',
        schedule_interval=None,
        default_args=default_args
    ) as dag:

    task1 = BashOperator(
            task_id='one_task',
            bash_command='echo "hello, world!"',
            dag=dag)