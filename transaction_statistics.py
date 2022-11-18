import sqlalchemy
from sqlalchemy import create_engine
import datetime

from airflow.models import Variable
from airflow import DAG


import pandas as pd
import boto3
import os

from airflow.operators.python import PythonOperator

session = boto3.session.Session()

ACCESS_ID = Variable.get("AWS_ACCESS_ID")
ACCESS_KEY = Variable.get("AWS_ACCESS_KEY")
dbConnection = None
file_path = "/opt/airflow/dags/statistics.csv" # so we can see it in our dags folder, should be something else in the real world

def ops_for_date_query(dt):
    next_dt = dt + datetime.timedelta(days=1)
    dt_formatted = dt.strftime("%Y-%m-%d")
    next_dt_formatted = next_dt.strftime("%Y-%m-%d")

    print(f"Generating query from date: {dt_formatted} to date {next_dt_formatted}")
    return sqlalchemy.text(f"""SELECT
ops."Id",
ops."TargetId",
ops."Entrypoint",
ops."Timestamp",
ops."Status",
ops."OpHash",
ops."Errors",
ops."InitiatorId",
acc."Id",
acc."Address",
acc2."Address" as "initiator_address"
FROM "TransactionOps" as ops
LEFT JOIN "Accounts" as acc
ON acc."Id" = ops."TargetId"
JOIN "Accounts" acc2 ON acc2."Id" = ops."InitiatorId"
WHERE ops."Status" = 1
AND ops."Timestamp" BETWEEN '{dt_formatted}' AND '{next_dt_formatted}'
ORDER BY ops."Timestamp" ASC
""")

def run_query():
    try:
        engine_params = Variable.get("INDEXER_CONNECTION_STRING")
        alchemyEngine = create_engine(
            engine_params, 
            pool_recycle=3600,
            pool_pre_ping=True,
            connect_args={
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5,
            })
        dbConnection = alchemyEngine.connect().execution_options(stream_results=True)
        print("dbConnection acquired")
        query = ops_for_date_query(datetime.datetime(2022, 10, 15, 0, 0))
        print("run query")
        blocks_df = pd.read_sql(query, dbConnection)
        print(blocks_df)

        blocks_df["ts"] = pd.to_datetime(blocks_df["Timestamp"])
        blocks_df["date"] = blocks_df["ts"].dt.strftime("%Y-%m-%d")


        blocks_df.to_csv(file_path, index=False)
    except Exception as e:
        print("exception when running the query", e)

def process_data(): # This should send the file to S3 bucket, now just confirms the file is there
    try:
        s3_client = session.client("s3",
            region_name="eu-west-1",
            aws_access_key_id=ACCESS_ID,
            aws_secret_access_key=ACCESS_KEY
        )
        s3_client.upload_file(
            file_path,
            "test-bucket-henri-1",
            "simple_blocks.csv",
        )

    except Exception as e:
        print("Exception when connecting to S3: ", e)
        exit(1)

def clean_up():
    os.remove(file_path)

with DAG(
    "transaction_statistics",
    default_args={'depends_on_past': False,
        'retries': 3,
        'retry_delay': datetime.timedelta(minutes=5),
    },
    description="Full history of statistics in Tezos chain",
    schedule=datetime.timedelta(days=1),
    start_date=datetime.datetime(2022, 10, 24),
    catchup=False,
    tags=["STATISTICS"]
) as dag:
    t1 = PythonOperator(
        task_id="run_query",
        python_callable=run_query
    )
    t2 = PythonOperator(
        task_id="process_data",
        python_callable=process_data
    )

    t3 = PythonOperator(
        task_id="clean_up",
        python_callable=clean_up
    )

    t1