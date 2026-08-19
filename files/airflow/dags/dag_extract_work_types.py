from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_extract_work_types"
POSTGRES_CONN_ID = "postgres_dwh"
ENTITY_NAME = "Catalog_ВидыРаботСотрудников"
RAW_TABLE = "raw.r1c_tech_operations"
DEFAULT_PAGE_SIZE = 1000


def get_cfg():
    return {
        "base_url": Variable.get("odata_1c_base_url"),
        "username": Variable.get("odata_1c_username"),
        "password": Variable.get("odata_1c_password"),
        "page_size": int(Variable.get("odata_1c_page_size", default_var=DEFAULT_PAGE_SIZE)),
        "timeout_sec": int(Variable.get("odata_1c_timeout_sec", default_var=120)),
    }


def fetch_work_types(**context):
    cfg = get_cfg()
    session = requests.Session()
    session.auth = (cfg["username"], cfg["password"])
    session.headers.update({"Accept": "application/json"})

    rows = []
    skip = 0
    select = "Ref_Key,DeletionMark,Description,Parent_Key,Code"

    while True:
        url = (
            f'{cfg["base_url"].rstrip("/")}/{ENTITY_NAME}'
            f"?$format=json&$select={select}"
            f"&$top={cfg['page_size']}&$skip={skip}"
        )
        response = session.get(url, timeout=cfg["timeout_sec"])
        response.raise_for_status()

        batch = response.json().get("value", [])
        if not batch:
            break

        rows.extend(
            (
                str(item.get("Ref_Key")),
                item.get("DeletionMark"),
                item.get("Description") or None,
                item.get("Parent_Key") or None,
                item.get("Code") or None,
            )
            for item in batch
        )

        if len(batch) < cfg["page_size"]:
            break
        skip += cfg["page_size"]

    logging.info("Fetched %s work types from %s", len(rows), ENTITY_NAME)
    context["ti"].xcom_push(key="work_type_rows", value=rows)
    context["ti"].xcom_push(key="work_type_count", value=len(rows))


def load_work_types(**context):
    rows = context["ti"].xcom_pull(
        task_ids="extract_work_types",
        key="work_type_rows",
    ) or []

    if not rows:
        raise ValueError("Catalog_ВидыРаботСотрудников returned zero rows")

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = f"""
        INSERT INTO {RAW_TABLE}
            (_id, _deletionmark, description, parent_id, operation_kind)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (_id) DO UPDATE SET
            _deletionmark = EXCLUDED._deletionmark,
            description = EXCLUDED.description,
            parent_id = EXCLUDED.parent_id,
            operation_kind = EXCLUDED.operation_kind,
            _loaded_at = now()
    """

    conn = pg.get_conn()
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()

    logging.info("Loaded %s rows into %s", len(rows), RAW_TABLE)


def quality_check(**context):
    fetched = context["ti"].xcom_pull(
        task_ids="extract_work_types",
        key="work_type_count",
    ) or 0

    if fetched == 0:
        raise ValueError("No work types fetched")

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    record = pg.get_first(
        """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE _deletionmark IS FALSE) AS active_rows
        FROM raw.r1c_tech_operations
        """
    )

    logging.info(
        "Work types quality check: fetched=%s, raw_total=%s, raw_active=%s",
        fetched,
        record[0],
        record[1],
    )


default_args = {
    "owner": "bi",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Полная выгрузка Catalog_ВидыРаботСотрудников из 1С OData",
    start_date=datetime(2026, 8, 19),
    schedule_interval="30 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["1c", "odata", "raw", "catalogs", "work-types"],
) as dag:
    extract = PythonOperator(
        task_id="extract_work_types",
        python_callable=fetch_work_types,
    )
    load = PythonOperator(
        task_id="load_work_types",
        python_callable=load_work_types,
    )
    check = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
    )

    extract >> load >> check
