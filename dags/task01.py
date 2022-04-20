import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd


def csv_to_json():
    df = pd.read_csv("dags/data/data.csv")

    for i, row in df.iterrows():
        print(row["Name"])

    df.to_json("dags/data/data.json", orient='records')


default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 4, 19),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

with DAG(
        'DAG_01',
        default_args=default_args,
        schedule_interval=timedelta(minutes=1),
        # '0 * * * *'
) as dag:
    print_starting = BashOperator(task_id='STARTING', bash_command="echo 'Reading CSV data...'")
    csv_json = PythonOperator(task_id='CSV_TO_JSON', python_callable=csv_to_json)

    # print_starting.set_downstream(csv_json)
    print_starting >> csv_json

if __name__ == '__main__':
    csv_to_json()
