from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago

import pandas as pd
import contextlib
import vertica_python
from typing import List, Optional, Dict





VERTICA_DWH_CONNECTION = "VERTICA_DWH_CONNECTION"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def load_dataset_file_to_vertica(
    dataset_path: str,
    schema: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,
):
    conn = BaseHook.get_connection(VERTICA_DWH_CONNECTION)
    df = pd.read_csv(dataset_path, dtype=type_override)

    if table == 'dialogs':
        df['message_group'] = pd.array(df['message_group'], dtype="Int64")
    if table == 'group_log':
        df['user_id_from'] = pd.array(df['user_id_from'], dtype="Int64")

    num_rows = len(df)
    vertica_conn = vertica_python.connect(
        host=str(conn.host),
        port=conn.port,
        user=str(conn.login),
        password=str(conn.password),
        db=str(conn.schema)
    )

    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY ''''
    """
    
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            print("loaded")
            start += chunk_size + 1

    vertica_conn.close()


with DAG('load_data_to_stg', 
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    
    load_users_data = PythonOperator(
        task_id='load_users',
        python_callable=load_dataset_file_to_vertica,
        op_args=('/data/users.csv', 'STV2024031233__STAGING', 'users', ['id', 'chat_name', 'registration_dt', 'country', 'age']),
    )

    load_dialogs_data = PythonOperator(
        task_id='load_dialogs',
        python_callable=load_dataset_file_to_vertica,
        op_args=('/data/dialogs.csv', 'STV2024031233__STAGING', 'dialogs', ['message_id', 'message_ts', 'message_from', 'message_to', 'message', 'message_group']),
    )

    load_groups_data = PythonOperator(
        task_id='load_groups',
        python_callable=load_dataset_file_to_vertica,
        op_args=('/data/groups.csv', 'STV2024031233__STAGING', 'groups', ['id', 'admin_id', 'group_name', 'registration_dt', 'is_private']),
    )

    load_group_log = PythonOperator(
        task_id='load_group_log',
        python_callable=load_dataset_file_to_vertica,
        op_args=('/data/group_log.csv', 'STV2024031233__STAGING', 'group_log', ['group_id','user_id','user_id_from','event','event_dt']),
    )


load_dialogs_data >> load_groups_data >> load_users_data >> load_group_log
