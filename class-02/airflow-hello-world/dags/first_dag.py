from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='hello_docker_airflow',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    task_1 = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello from Airflow in Docker!"'
    )

    task_2 = BashOperator(
        task_id='show_date',
        bash_command='date'
    )

    task_1 >> task_2
