from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from Subdags.subdag_downloads import subdag_downloads  # Importing "subdag_downloads" function present inside subdag_downloads file of Subdag Folder.
from Subdags.subdag_transforms import transforms  # Importing "transforms" function present inside subdag_transforms file of Subdag Folder.

from datetime import datetime

with DAG ('GROUP_DAG2', start_date = datetime(2023,5,27),
    schedule_interval = '@daily', catchup = False) as dag:

    args = {'start_date':dag.start_date,'schedule_interval':dag.schedule_interval,'catchup':dag.catchup}

    downloads = SubDagOperator(
        task_id = 'downloads',
        subdag = subdag_downloads(dag.dag_id,'downloads',args)
    )


    check_files = BashOperator(
        task_id = 'check_files',
        bash_command = 'sleep 10'
    )

    transforms = SubDagOperator(
        task_id = 'transforms',
        subdag = transforms(dag.dag_id,'transforms',args)
    )

    start = DummyOperator(task_id = 'start', dag=dag)
    end = DummyOperator(task_id ='end', dag=dag)

    start >> downloads >> check_files >> transforms >>end
    


