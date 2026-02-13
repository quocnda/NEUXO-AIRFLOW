from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

def count_1_to_100():
    for i in range(1, 101):
        print(f"Counting: {i}")
        time.sleep(1)  # sleep nhẹ để thấy log

with DAG(
    dag_id="count_1_to_100_every_10_minutes",
    start_date=datetime(2025, 1, 1),
    schedule="*/10 * * * *",   # mỗi 10 phút
    catchup=False,
    tags=["demo", "basic"],
) as dag:

    count_task = PythonOperator(
        task_id="count_numbers",
        python_callable=count_1_to_100,
    )
