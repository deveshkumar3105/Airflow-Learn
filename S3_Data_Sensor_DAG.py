from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta
import pandas as pd


def read_s3_file():
    s3_hook = S3Hook(aws_conn_id='my_s3_conn')
    amazon_data = s3_hook.read_key('amazon.csv', bucket_name='amazon-sales-dataset')
    return amazon_data


def deduplicate_data(**context):
    ti = context['ti']
    amazon_data = ti.xcom_pull(task_ids='read_s3_file_data')

    df = pd.DataFrame.from_records(amazon_data)
    df = df.drop_duplicates()
    cleaned_data = df.to_dict('records')
    return cleaned_data


default_args = {
    'owner': 'Devesh',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 20),
    'email': ['deveshkumar3105@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('read_s3_file', default_args=default_args, schedule_interval='@daily')

s3_sensor_task = S3KeySensor(
    task_id='s3_sensor_task',
    poke_interval=60,
    timeout=60*60*24,
    bucket_key='amazon.csv',
    bucket_name='amazon-sales-dataset',
    wildcard_match=True,
    aws_conn_id='my_s3_conn',
    dag=dag
)

read_s3_file_data = PythonOperator(
    task_id='read_s3_file_data',
    python_callable=read_s3_file,
    dag=dag
)

data_deduplication = PythonOperator(
    task_id='data_deduplication',
    python_callable=deduplicate_data,
    provide_context=True,
    dag=dag
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> s3_sensor_task >> read_s3_file_data >> data_deduplication >> end
