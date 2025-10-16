from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '* * * * *',
}

def clickhouse_connect_and_query():
    """Подключение к ClickHouse и выполнение запроса"""
    client = Client(
        host='localhost',
        port=9000,
        user='admin',
        password='clickhouse_password',
        database='default'
    )

    # Создание таблицы
    client.execute('''
        CREATE TABLE IF NOT EXISTS test_table (
            id Int32,
            name String,
            timestamp DateTime
        ) ENGINE = MergeTree()
        ORDER BY id
    ''')

    # Вставка данных
    data = [(1, 'test1', datetime.now()), (2, 'test2', datetime.now())]
    client.execute('INSERT INTO test_table VALUES', data)

    # Чтение данных
    result = client.execute('SELECT * FROM test_table')
    print("Данные из ClickHouse:", result)

with DAG(
    'clickhouse_example',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    run_clickhouse_query = PythonOperator(
        task_id='run_clickhouse_query',
        python_callable=clickhouse_connect_and_query
    )

    run_clickhouse_query
