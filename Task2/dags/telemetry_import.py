from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from clickhouse_driver import Client
from datetime import datetime, timedelta
import pandas as pd
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '* * * * *',
}

# Функция для чтения данные и генерации SQL запросов
def generate_insert_queries():
    JSON_FILE_PATH = 'sample_files/telemetry_import.json'
    with open( JSON_FILE_PATH, 'r') as jsonfile:
        data = json.load(jsonfile)
        postgres_hook = PostgresHook(postgres_conn_id='write_to_postgres')
        # Генерим запросы
        insert_queries = []
        for row in data['telemetry']:
            device_id = row['device_id']
            timestamp = row['timestamp']
            telemetry = json.dumps(row['telemetry'])
            df = postgres_hook.get_pandas_df(f"SELECT email FROM crm_client WHERE device_id='{device_id}';")
            email = df.loc[0, 'email']
            insert_query = f"INSERT INTO device_telemetry(device_id,created_at,telemetry,user_email) VALUES ('{device_id}',fromUnixTimestamp({timestamp}),'{telemetry}','{email}');"
            insert_queries.append(insert_query)

        # Сохраняем запросы
        with open('./dags/sql/device_telemetry_insert_queries.sql', 'w') as f:
            for query in insert_queries:
                f.write(f"{query}\n")

def create_clickhose_table():
    client = Client(
        host='clickhouse',
        port=9000,
        user='admin',
        password='clickhouse_password',
        database='default'
    )

    # Создание таблицы
    client.execute('''
        DROP TABLE device_telemetry;
        CREATE TABLE IF NOT EXISTS device_telemetry (
            user_email String,
            device_id String,
            created_at DateTime,
            telemetry String
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(created_at)
        ORDER BY (device_id, created_at);
    ''')

def clickhouse_connect_and_query():
    """Подключение к ClickHouse и выполнение запроса"""
    client = Client(
            host='clickhouse',
            port=9000,
            user='admin',
            password='clickhouse_password',
            database='default'
    )

    # Вставка данных
    with open('./dags/sql/device_telemetry_insert_queries.sql', 'r') as sqlfile:
        sql = sqlfile.read()
        client.execute(sql)

with DAG(
    'clickhouse_example',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
) as dag:

    generate_sql = PythonOperator(
        task_id='generate_sql',
        python_callable=generate_insert_queries
    )
    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_clickhose_table
    )
    run_clickhouse_query = PythonOperator(
        task_id='run_clickhouse_query',
        python_callable=clickhouse_connect_and_query
    )

    generate_sql>>create_table>>run_clickhouse_query
