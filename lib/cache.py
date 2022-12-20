import pandas as pd
import os
import glob

base_path = "/opt/airflow/dags/cache"

def store_df(df, key, kwargs, is_global=False): # is_global means that the value is shared by different subdags
    run_id, dag_id = extract_ids(kwargs)

    path = f"{base_path}/{run_id}"
    if os.path.isdir(path) is False:
        os.mkdir(path)

    if is_global:
        file_name = f"{key}.csv"
    else:
        file_name = f"{dag_id}_{key}.csv"
    
    df.to_csv(f"{path}/{file_name}", index=True)
    

def get_df(key, kwargs, is_global=False):
    run_id, dag_id = extract_ids(kwargs)

    path = f"{base_path}/{run_id}"

    if is_global:
        return pd.read_csv(f"{path}/{key}.csv")
    
    return pd.read_csv(f"{path}/{dag_id}_{key}.csv")

def collect_results(run_id):
    file_names = glob.glob(f"{base_path}/{run_id}/*results.csv")
    print("file names: ", file_names)
    frames = []

    for file in file_names:
        df = pd.read_csv(file)
        frames.append(df)

    return frames

def extract_ids(kwargs):
    return kwargs["dag_run"].run_id, kwargs['dag'].dag_id