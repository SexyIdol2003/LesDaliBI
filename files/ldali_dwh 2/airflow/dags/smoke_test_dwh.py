"""DAG smoke-теста: проверяет, что Airflow видит DWH и все схемы созданы."""
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


def check_dwh():
    hook = PostgresHook(postgres_conn_id="pg_dwh")
    rows = hook.get_records(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name IN ('raw','staging','mart','meta') "
        "ORDER BY schema_name"
    )
    schemas = [r[0] for r in rows]
    print(f"Найденные схемы DWH: {schemas}")
    expected = {"mart", "meta", "raw", "staging"}
    missing = expected - set(schemas)
    if missing:
        raise RuntimeError(f"Отсутствуют схемы: {missing}")
    # сколько уже dim/fact таблиц в mart
    cnt = hook.get_first(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema='mart' AND table_type='BASE TABLE'"
    )[0]
    print(f"Таблиц в mart: {cnt}")
    print("DWH готов к работе.")


with DAG(
    dag_id="smoke_test_dwh",
    description="Проверка связи Airflow ↔ Postgres DWH и наличия слоёв",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ldali", "smoke"],
) as dag:
    PythonOperator(task_id="check_dwh_schemas", python_callable=check_dwh)
