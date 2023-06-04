from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


def _t1(ti):
        ti.xcom_push(key='my_key',value=46)

def _t2(ti):
        ti.xcom_pull(key='my_key_2',task_ids='t1')

def _branch(ti):
    value = ti.xcom_pull(key='my_key',task_ids='t1')
    if (value==45):
        return 't2'
    return 't3'

with DAG(
    'Branch_N_Trigger_DAG',
    start_date=datetime(2023,5,27),
    schedule_interval='48 5 * * 7', 
    tags = ['Branch Operator and Trigger Rule']
    ) as dag:

    t1 = PythonOperator(
        task_id = 't1',
        python_callable = _t1
    )

    t2 = PythonOperator(
        task_id = 't2',
        python_callable = _t2
    )

    branch = BranchPythonOperator(
        task_id = 'branch',
        python_callable = _branch
    )

    t3 = BashOperator(
        task_id = 't3',
        bash_command = "echo 'Hi Devesh'"
    )

    t4 = BashOperator(
        task_id = 't4',
        bash_command = "echo 'Moved Ahead'",
        trigger_rule = "none_failed_min_one_success"
        #rigger_rule = "all_success"
    )

    t1 >> branch >> [t2,t3] >> t4

    