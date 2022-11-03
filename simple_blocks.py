import sqlalchemy
from sqlalchemy import create_engine
from datetime import timedelta, datetime

from airflow.models import Variable
from airflow import DAG

import csv

import pandas as pd
import boto3

from airflow.operators.python import PythonOperator

file_path = "/opt/airflow/dags/block_stats.csv" # so we can see it in our dags folder, should be something else in the real world

def run_query():
    engine_params = Variable.get("INDEXER_CONNECTION_STRING")
    alchemyEngine = create_engine(engine_params, pool_recycle=3600)
    dbConnection = alchemyEngine.connect().execution_options(stream_results=True)
    print("dbConnection acquired")
    get_all_blocks_query = sqlalchemy.text("""
    SELECT "Level", "Timestamp", "Id" FROM public."Blocks"
    ORDER BY "Level" ASC
    """)
    print("get all blocks query")
    blocks_df = pd.read_sql(get_all_blocks_query, dbConnection)
    print(blocks_df)

    blocks_df["ts"] = pd.to_datetime(blocks_df["Timestamp"])
    blocks_df["date"] = blocks_df["ts"].dt.strftime("%Y-%m-%d")

    block_stats_per_day = blocks_df.groupby("date").size().reset_index(name="blocks")

    print(block_stats_per_day)

    block_stats_per_day.to_csv(file_path, index=False)

def process_data(): # This should send the file to S3 bucket, now just confirms the file is there
    with open(file_path) as f:
        csv_reader = csv.reader(f, delimiter=",")
        for row in csv_reader:
            print(row)
            return

with DAG(
    "simple_blocks_stats",
    default_args={'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description="Simple script to load the blocks table from tzkt indexer postgres database, calculate daily statistics for blocks and push the output file to S3",
    schedule=timedelta(days=1),
    start_date=datetime(2022, 10, 24),
    catchup=False,
    tags=["BLOCKS"]
) as dag:
    t1 = PythonOperator(
        task_id="run_query",
        python_callable=run_query
    )
    t2 = PythonOperator(
        task_id="process_data",
        python_callable=process_data
    )

    t1 >> t2