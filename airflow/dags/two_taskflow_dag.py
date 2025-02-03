from __future__ import annotations

import logging
import time

import pendulum

from airflow.decorators import dag, task

log = logging.getLogger(__name__)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"]
)
def two_taskflow_dag():
    # [START howto_operator_python]
    @task(task_id="print_first_task_output")
    def firstTask():
        log.info("First Airflow task completed")

    first_Task = firstTask()

    @task(task_id="print_second_task_output")
    def secondTask():
        log.info("Sleeping...")
        time.sleep(5)
        return "Second Airflow task"

    second_Task = secondTask()

    first_Task >> second_Task

two_taskflow_dag()