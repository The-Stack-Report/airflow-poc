import pandas as pd
import os
import glob

class Cache():
    def __init__(self, dag_id) -> None:
        self.dag_id = dag_id
        self.paths_to_destroy = []

    def __del__(self):
        for path in self.paths_to_destroy:
            os.remove(path)

    def store_df(self, key, df, persist=False):
        path = f"/opt/airflow/dags/cache/{self.dag_id}_{key}.csv"
        df.to_csv(path, index=True)

        if persist is False:
            self.paths_to_destroy.append(path)

    def read_df(self, key):
        df = pd.read_csv(f"/opt/airflow/dags/cache/{self.dag_id}_{key}.csv")
        return df

    def store_global_df(self, key, df, is_result=False):
        if is_result:
            path = f"/opt/airflow/dags/cache/result_{self.dag_id}_{key}.csv"
        else:
            path = f"/opt/airflow/dags/cache/global_{key}.csv"
        df.to_csv(path, index=True)

    def read_global_df(self, key):
        path = f"/opt/airflow/dags/cache/global_{key}.csv"
        df = pd.read_csv(path)
        return df


def collect_results():
    file_names = glob.glob("/opt/airflow/dags/cache/result_*.csv")
    print("file names: ", file_names)
    frames = []

    for file in file_names:
        df = pd.read_csv(file)
        frames.append(df)

    return frames