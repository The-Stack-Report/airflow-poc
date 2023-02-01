import datetime

from airflow.models import Variable
from airflow import DAG

from lib.db import ops_for_date_query
from lib.cache import get_df, store_df
from lib.s3 import upload_to_bucket

import pandas as pd

from airflow.operators.python import PythonOperator



dbConnection = None

def run_query(**kwargs):
    try:
        if "connection" not in kwargs:
            raise Exception("No connection in arguments")

        conCon = kwargs["connection"]
        dbConnection = conCon.get_connection()
        print("run query: ", dbConnection)

        query = ops_for_date_query(datetime.datetime(2022, 10, 15, 0, 0))
        print("run query: ", query)
        ops_df = pd.read_sql(query, dbConnection)
        print(ops_df)

        store_df(ops_df, "day_ops", kwargs)
        del ops_df
        
    except Exception as e:
        print("exception when running the query", e)


def enrich_data(**kwargs):
    ops_for_day_df = get_df("day_ops", kwargs)
    accounts_df = get_df("accounts", kwargs, True)

    ids_for_day = pd.unique(ops_for_day_df[["TargetId", "SenderId", "InitiatorId"]].values.ravel("K"))
    accounts_for_day_df = accounts_df[accounts_df["Id"].isin(ids_for_day)]
    addresses_by_id_for_day = dict(zip(accounts_for_day_df["Id"], accounts_for_day_df["Address"]))
    
    ops_for_day_df["target_address"] = ops_for_day_df["TargetId"] # Sender address
    ops_for_day_df["sender_address"] = ops_for_day_df["SenderId"] # Initiator address
    ops_for_day_df["initiator_address"] = ops_for_day_df["InitiatorId"] # Target address

    ops_for_day_df.replace({
        "target_address": addresses_by_id_for_day,
        "sender_address": addresses_by_id_for_day,
        "initiator_address": addresses_by_id_for_day
    }, inplace=True)

    ops_for_day_df.sort_values(by="Id", ascending=True, inplace=True)
    store_df(ops_for_day_df, "enriched", kwargs)

def check(df):
    # Validating that each op group has only 1 wallet which is sending transactions in each transaction group.

    # If this is the case we can use unique nr of wallets sending transactions as wallet metric.
    wallet_sent_ops = df[df["sender_address"].str.startswith("tz")]

    sender_unique_per_op_group_hash = wallet_sent_ops.groupby("OpHash").agg({"sender_address": "nunique"})

    sender_unique_per_op_group_hash.reset_index()

    min_wallets = int(sender_unique_per_op_group_hash["sender_address"].min())
    max_wallets = int(sender_unique_per_op_group_hash["sender_address"].max())

    if not min_wallets > 0:
        raise ValueError(f"Minimum nr of unique wallets sending operations per operation group expected to be 1, but is {min_wallets}")
    if not max_wallets < 5:
        raise ValueError(f"Maximum nr of unique wallets sending operations per operation group expected to be 1, but is {max_wallets}")

    if max_wallets > 1:
        print(df)
        print(f"Df contains transaction groups with more than 1 sender wallet. Max found is: {max_wallets}")
    print("All operation groups contain only between 1 and 5 unique wallet sender address")
    return True


def analyze_data(**kwargs):
    df = get_df("enriched", kwargs)
    if check(df) is False:
        raise ValueError("Data frame did not pass checks")

    # Wallet sender group
    wallet_sender_df = df[df["sender_address"].str.startswith("tz")]
    wallet_to_wallet_df = wallet_sender_df[wallet_sender_df["target_address"].str.startswith("tz")]
    wallet_to_contract_df = wallet_sender_df[wallet_sender_df["target_address"].str.startswith("KT")]

    unique_wallet_senders = [addr for addr in df["sender_address"].values if addr.startswith("tz")]
    unique_wallets_targetted = [addr for addr in df["target_address"].values if addr.startswith("tz")]

    total_unique_wallets = set([*unique_wallet_senders, *unique_wallets_targetted])

    # Contract sender group
    contract_sender_df = df[df["sender_address"].str.startswith("KT")]
    contract_to_contract_df = contract_sender_df[contract_sender_df["target_address"].str.startswith("KT")]
    contract_to_wallet_df = contract_sender_df[contract_sender_df["target_address"].str.startswith("tz")]

    # Wallet targetted
    wallet_targeted_df = df[df["target_address"].str.startswith("tz")]

    # Contract targetted
    contract_targeted_df = df[df["target_address"].str.startswith("KT")]

    # Contract entrypoint calls
    entrypoint_calls_df = df[df["Entrypoint"].notnull()]

    # Transaction groups with contract calls
    ophashes_with_entrypoint = entrypoint_calls_df["OpHash"].unique()

    entrypoint_call_transactions_count = len(entrypoint_calls_df)

    transaction_groups_with_entrypoint_calls = len(ophashes_with_entrypoint)

    transactions_count = len(df)
    transactions_groups_count = len(df["OpHash"].unique())
    transactions_in_groups_with_entrypoint_calls_df = df[df["OpHash"].isin(ophashes_with_entrypoint)]
    transactions_in_groups_with_entrypoint_calls = len(transactions_in_groups_with_entrypoint_calls_df)

    smart_contract_transactions_to_transaction_groups_ratio = False
    if(transaction_groups_with_entrypoint_calls > 0):
        smart_contract_transactions_to_transaction_groups_ratio = transactions_in_groups_with_entrypoint_calls / transaction_groups_with_entrypoint_calls
    
    stats = {
        "transactions": transactions_count,
        "wallet_sender_transactions": len(wallet_sender_df),
        "contract_sender_transactions": len(contract_sender_df),
        "wallet_targeted_transactions": len(wallet_targeted_df),
        "wallet_targeted_transactions": len(wallet_targeted_df),
        "contract_targeted_transactions": len(contract_targeted_df),
        "wallet_to_wallet_transactions": len(wallet_to_wallet_df),
        "wallet_to_contract_transactions": len(wallet_to_contract_df),
        "contract_to_contract_transactions": len(contract_to_contract_df),
        "contract_to_wallet_transactions": len(contract_to_wallet_df),
        "transaction_groups": transactions_groups_count,
        "transactions_to_groups_ratio": transactions_count / transactions_groups_count, 
        "smart_contract_transactions_to_groups_ratio": smart_contract_transactions_to_transaction_groups_ratio,

        "wallets_sending_transactions": len(wallet_sender_df["sender_address"].unique()),
        "wallets_calling_contracts": len(wallet_to_contract_df["sender_address"].unique()),
        "contracts_sending_transactions": len(contract_sender_df["sender_address"].unique()),
        "wallets_involved_in_transactions": len(total_unique_wallets),

        "entrypoint_call_transactions": entrypoint_call_transactions_count,
        "transactions_in_groups_with_entrypoint_calls": transactions_in_groups_with_entrypoint_calls,
        "transaction_groups_with_entrypoint": transaction_groups_with_entrypoint_calls,

        "gas_used_sum": df["GasUsed"].sum() / 1,
        "gas_used_max": df["GasUsed"].max() / 1,
        "gas_used_median": df["GasUsed"].median() / 1,
        "gas_used_mean": df["GasUsed"].mean() / 1,

        "baker_fee_xtz_sum": round(df["BakerFee"].sum() / 1_000_000, 1),
        "baker_fee_xtz_max": df["BakerFee"].max() / 1_000_000,
        "baker_fee_xtz_median": df["BakerFee"].median() / 1_000_000,
        "baker_fee_xtz_mean": df["BakerFee"].mean() / 1_000_000,
    }
    result_df = pd.DataFrame(stats, index=[0])
    store_df(result_df, "results", kwargs)
    upload_to_bucket(
        "the-stack-report-prototyping", 
        f"{kwargs['dag'].dag_id}_result",
        result_df
        )

def transaction_statistics_day(parent_dag_name, child_dag_name, args):
    dag = DAG(default_args={'depends_on_past': False,
            'retries': 3,
            'retry_delay': datetime.timedelta(minutes=5),
        },
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        description="Full history of statistics in Tezos chain",
        schedule=datetime.timedelta(days=1),
        start_date=datetime.datetime(2022, 10, 24),
        catchup=False,
        tags=["STATISTICS"])
    
    run_initial_query = PythonOperator(
        task_id="run_query",
        python_callable=run_query,
        dag=dag,
        op_kwargs=args
        
    )
    process = PythonOperator(
        task_id="process_data",
        python_callable=analyze_data,
        dag=dag,
        op_kwargs=args
    )

    enrich = PythonOperator(
        task_id="enrich_data",
        python_callable=enrich_data,
        dag=dag,
        op_kwargs=args
    )

    # t3 = PythonOperator(
    #     task_id="clean_up",
    #     python_callable=clean_up
    # )

    run_initial_query >> enrich >> process
    
    return dag