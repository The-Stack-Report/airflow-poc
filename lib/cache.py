import pandas as pd
import os

class Cache():
    def __init__(self, dag_id) -> None:
        self.dag_id = dag_id
        self.paths_to_destroy = []

    def __del__(self):



    def store_df(self, key, df, persist=False):
        path = f"/opt/airflow/dags/cache/{self.dag_id}_{key}.csv"
        df.to_csv(path, index=True)

        if persist is False:
            self.paths_to_destroy.append(path)


    def read_df(self, key):
        df = pd.read_csv(f"/opt/airflow/dags/cache/{self.dag_id}_{key}.csv")
        return df

