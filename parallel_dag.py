from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime

with DAG ('PARALLEL_DAG',start_date = datetime(2023,5,27),
    schedule_interval='@daily', catchup=False) as dag:

    extract_a = BashOperator(
        task_id = 'extract_a',
        bash_command = 'sleep 1'
    )

    extract_b = BashOperator(
        task_id = 'extract_b',
        bash_command = 'sleep 1'
    )

    load_a = BashOperator(
        task_id = 'load_a',
        bash_command = 'sleep 1'
    )

    load_b = BashOperator(
        task_id = 'load_b',
        bash_command = 'sleep 1'
    )

    transform = BashOperator(
        task_id = 'transform',
        bash_command = 'sleep 1'
    )

    start = DummyOperator(task_id='start',dag=dag)
    end = DummyOperator(task_id = 'end',dag=dag)

    start >> [extract_a,extract_b]
    extract_a >> load_a
    extract_b >> load_b
    [load_a,load_b] >> transform >> end

