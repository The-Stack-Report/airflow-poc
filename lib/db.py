from airflow.models import Variable
from sqlalchemy import create_engine

import sqlalchemy
import datetime

class ConnectionContainer():
    def __init__(self):
        self.connection = None

    def get_connection(self):
        print("getConnection!")
        if self.connection is None:
            print("create connection")
            self.connect()
        
        return self.connection
    
    def connect(self):
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

            self.connection = dbConnection
        except Exception as e:
            print("Exception when connecting to db: ", e)
            raise Exception("Couldn't connect to db")


def ops_for_date_query(dt):
    next_dt = dt + datetime.timedelta(days=1)
    dt_formatted = dt.strftime("%Y-%m-%d")
    next_dt_formatted = next_dt.strftime("%Y-%m-%d")

    print(f"Generating query from date: {dt_formatted} to date {next_dt_formatted}")
    return sqlalchemy.text(f"""SELECT
ops."Id",
ops."TargetId",
ops."Entrypoint",
ops."Amount",
ops."Timestamp",
ops."Status",
ops."OpHash",
ops."Errors",
ops."SenderId",
ops."InitiatorId",
ops."BakerFee",
ops."StorageFee",
ops."AllocationFee",
ops."GasUsed",
ops."GasLimit",
ops."Level"
FROM "TransactionOps" as ops
WHERE ops."Status" = 1
AND ops."Timestamp" BETWEEN '{dt_formatted}' AND '{next_dt_formatted}'
ORDER BY ops."Timestamp" ASC
""")