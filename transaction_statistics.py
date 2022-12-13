from airflow.models import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from lib.db import ConnectionContainer, ops_for_date_query
from lib.cache import Cache, collect_results

import datetime

from transaction_statistics_day import transaction_statistics_day

DAG_ID="transaction_statistics"

from airflow.utils.dates import days_ago
import pandas as pd


connection = ConnectionContainer()

def process_results():
    print("process results")
    frames = collect_results()
    print(frames)
    df = pd.concat(frames)
    print(df)
    df.to_csv("/opt/airflow/dags/cache/final_result.csv", index=True)
    print("wrote file so it should be there now...")

def prepare(**kwargs):
    cache = Cache("prepare")
    accounts_query = """
SELECT Accounts."Id", Accounts."Address" FROM public."Accounts" as Accounts
ORDER BY "Id" ASC"""

    accounts_df = pd.read_sql(accounts_query, kwargs["connection"].get_connection())
    cache.store_global_df("accounts", accounts_df)

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
    tasks = [
        PythonOperator(
            task_id="prepare_task",
            dag=dag,
            python_callable=prepare,
            op_kwargs={
                "connection": connection
            }
        )
    ]

    for i in range(2):
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

    process_results_task = PythonOperator(
        task_id="process_results",
        dag=dag,
        python_callable=process_results,
    )

    tasks[-1].set_downstream(collect_results)
    collect_results.set_upstream(tasks[-1])