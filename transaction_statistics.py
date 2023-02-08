from airflow.models import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from lib.db import ConnectionContainer, ops_for_date_query
from lib.cache import store_df, collect_results

import datetime

from transaction_statistics_day import transaction_statistics_day

DAG_ID="transaction_statistics"

from airflow.utils.dates import days_ago

import pandas as pd
import time


connection = ConnectionContainer()

def prepare(**kwargs):
    accounts_query = """
SELECT Accounts."Id", Accounts."Address" FROM public."Accounts" as Accounts
ORDER BY "Id" ASC"""

    accounts_df = pd.read_sql(accounts_query, kwargs["connection"].get_connection())
    store_df(accounts_df, "accounts", kwargs, True)


def process_results(**kwargs):
    run_id = kwargs["dag_run"].run_id
    print("process results")
    frames = collect_results(run_id)
    print(frames)
    df = pd.concat(frames)
    print(df)
    df.to_csv(f"/opt/airflow/dags/cache/final_result_{run_id}.csv", index=True)


with DAG(
    dag_id=DAG_ID,
    default_args={
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': datetime.timedelta(minutes=5)
    },
    description="Full history of statistics in Tezos chain",
    start_date=datetime.datetime(2022, 10, 24),
    catchup=False,
    tags=["STATISTICS"],
) as dag:
    args = {
            "connection": connection,
            "timestamp": int(time.time()),
            "dag_id": DAG_ID
        }

    tasks = [
        PythonOperator(
            task_id="prepare_task",
            dag=dag,
            python_callable=prepare,
            op_kwargs=args
        )
    ]

    for i in range(2):
        args["days_ago"] = days_ago(10-i)
        datestring = args["days_ago"].strftime('%m-%d-%Y')
        task_id = f"day_operation_{datestring}"
        day_statistics = SubDagOperator(
            task_id=task_id,
            subdag=transaction_statistics_day(DAG_ID, task_id, args),
            dag=dag
        )

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