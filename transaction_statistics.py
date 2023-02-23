from airflow.models import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from lib.db import ConnectionContainer, ops_for_date_query
from lib.cache import store_df, collect_results

from lib.s3 import get_missing_dates

import datetime
import random
import string

from transaction_statistics_day import transaction_statistics_day

DAG_ID="transaction_statistics"

from airflow.utils.dates import days_ago

import pandas as pd
import time


connection = ConnectionContainer()

def process_results(**kwargs):
    run_id = kwargs["dag_run"].run_id
    frames = collect_results(run_id)
    df = pd.concat(frames)
    df.to_csv(f"/opt/airflow/dags/cache/final_result_{run_id}.csv", index=True)


with DAG(
    dag_id=DAG_ID,
    default_args={
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': datetime.timedelta(minutes=1)
    },
    description="Full history of statistics in Tezos chain",
    start_date=datetime.datetime(2023, 2, 15),
    catchup=False,
    tags=["STATISTICS"],
    is_paused_upon_creation=False
) as dag:
    args = {
            "connection": connection,
            "timestamp": int(time.time()),
            "dag_id": DAG_ID
        }


    tasks = []

    dates = get_missing_dates("2022-12-1", "2023-02-24")

    for date in dates:
        args["date"] = date
        datestring = date.strftime('%Y-%m-%d')
        task_id = f"day_operation_{datestring}"
        day_statistics = SubDagOperator(
            task_id=task_id,
            subdag=transaction_statistics_day(DAG_ID, task_id, args),
            dag=dag
        )
        if len(tasks) > 0:
            tasks[-1].set_downstream(day_statistics)
            day_statistics.set_upstream(tasks[-1])

        tasks.append(day_statistics)

    process_results_task = PythonOperator(
        task_id="process_results",
        dag=dag,
        python_callable=process_results,
        op_kwargs=args
    )

    tasks[-1].set_downstream(process_results_task)
    process_results_task.set_upstream(tasks[-1])