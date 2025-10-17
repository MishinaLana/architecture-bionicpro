from clickhouse_driver import Client
from airflow.providers.postgres.hooks.postgres import PostgresHook

# postgres_hook = PostgresHook(postgres_conn_id='write_to_postgres')
# device_id = "BIO-A71-2284"
# df = postgres_hook.get_pandas_df(f"SELECT email FROM crm_client WHERE device_id='{device_id}'")
# email = df.loc[0, 'email']
# print(email)
# client = Client(
#         host='clickhouse',
#         port=9000,
#         user='admin',
#         password='clickhouse_password',
#         database='default'
# )

# client.execute('''
#         DROP TABLE device_telemetry
#
#     ''')
#
# client.execute('''
#         CREATE TABLE device_telemetry (
#             device_id String,
#             created_at DateTime,
#             telemetry String
#         ) ENGINE = MergeTree()
#         PARTITION BY toYYYYMM(created_at)
#         ORDER BY (device_id, created_at);
#     ''')

with open('./sql/device_telemetry_insert_queries.sql', 'r') as sqlfile:
        sql = sqlfile.read()
        print(sql)
        client.execute(sql)

# result = client.execute("select * from device_telemetry")

# print(result)
