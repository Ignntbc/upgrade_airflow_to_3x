"""Простейший пример DAG для проверки CeleryExecutor."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


def _print_hello() -> str:
    print("Hello from Celery worker!")
    return "ok"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="example_celery_smoke",
    description="Smoke-тест воркеров Celery",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "smoke"],
) as dag:

    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=_print_hello,
    )

    show_hostname = BashOperator(
        task_id="show_hostname",
        bash_command="echo Worker hostname: $HOSTNAME && sleep 5",
    )

    say_hello >> show_hostname
