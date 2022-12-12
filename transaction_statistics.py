from airflow.models import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from lib.db import ConnectionContainer, ops_for_date_query

import datetime

from transaction_statistics_day import transaction_statistics_day

DAG_ID="transaction_statistics"

from airflow.utils.dates import days_ago


connection = ConnectionContainer()

def prepare():
    print("PREPARE!")

with DAG(
    dag_id=DAG_ID,
    default_args={
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': datetime.timedelta(minutes=5)
    },
    description="Full history of statistics in Tezos chain",
    schedule=datetime.timedelta(days=1),
    start_date=datetime.datetime(2022, 10, 24),
    catchup=False,
    tags=["STATISTICS"],
) as dag:
    print("created connection")

    tasks = [
        PythonOperator(
            task_id="prepare_task",
            dag=dag,
            python_callable=prepare
        )
    ]

    for i in range(3):
        task_id = f"day_operation_{i}"
        args = {
            "days_ago": days_ago(10-i),
            "connection": connection
        }
        day_statistics = SubDagOperator(
            task_id=task_id,
            subdag=transaction_statistics_day(DAG_ID, task_id, args),
            dag=dag
        )

        tasks[-1].set_downstream(day_statistics)
        day_statistics.set_upstream(tasks[-1])

        tasks.append(day_statistics)
