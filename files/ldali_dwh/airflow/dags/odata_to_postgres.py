from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests

ODATA_BASE = "http://10.50.254.22/ld_erp_ibiOdata/odata/standard.odata"
ODATA_USER = "БортковИИ"
ODATA_PASS = "xxx123"

ENTITIES = [
    "Catalog_Валюты",
    "Catalog_Номенклатура",
    "Catalog_Контрагенты",
    "Catalog_ПодразделенияОрганизаций",
    "Catalog_Склады",
]

def fetch_and_load(entity: str):
    pg = PostgresHook(postgres_conn_id="postgres_dwh")
    url = f"{ODATA_BASE}/{entity}"
    page_size = 1000
    skip = 0
    rows = []

    import base64
    auth_header = "Basic " + base64.b64encode(f"{ODATA_USER}:{ODATA_PASS}".encode("utf-8")).decode()

    while True:
        params = {"$format": "json", "$top": page_size, "$skip": skip}
        r = requests.get(url, params=params, headers={"Authorization": auth_header}, timeout=120)
        r.raise_for_status()
        data = r.json()
        page = data.get("value", [])
        rows.extend(page)
        if len(page) < page_size:
            break
        skip += page_size

    pg.run("CREATE SCHEMA IF NOT EXISTS raw")
    table = f"raw.{entity.lower()}"
    pg.run(f"DROP TABLE IF EXISTS {table}")
    if rows:
        import json
        def to_str(v):
            if isinstance(v, (dict, list)):
                return json.dumps(v, ensure_ascii=False)
            return v
        seen = {}
        safe_keys = []
        for k in rows[0].keys():
            base = k.encode('utf-8')[:59].decode('utf-8', 'ignore')
            n = seen.get(base, 0)
            seen[base] = n + 1
            safe_keys.append(base if n == 0 else f'{base}_{n}')
        key_map = dict(zip(rows[0].keys(), safe_keys))
        create_cols = ', '.join(f'"{key_map[k]}" TEXT' for k in rows[0].keys())
        pg.run(f"CREATE TABLE {table} ({create_cols})")
        insert_cols = ', '.join(f'"{key_map[k]}"' for k in rows[0].keys())
        placeholders = ', '.join(['%s'] * len(rows[0]))
        insert_sql = f'INSERT INTO {table} ({insert_cols}) VALUES ({placeholders})'
        keys = list(rows[0].keys())
        values = [tuple(to_str(r.get(k)) for k in keys) for r in rows]
        conn = pg.get_conn()
        cur = conn.cursor()
        cur.executemany(insert_sql, values)
        conn.commit()
        cur.close()
        conn.close()

with DAG(
    dag_id="odata_1c_to_raw",
    start_date=datetime(2026, 6, 30),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:
    for entity in ENTITIES:
        PythonOperator(
            task_id=f"load_{entity}",
            python_callable=fetch_and_load,
            op_args=[entity],
        )
