from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_extract_nomenclature"
POSTGRES_CONN_ID = "postgres_dwh"
DEFAULT_PAGE_SIZE = 1000

ENTITY = "Catalog_Номенклатура"
SELECT_FIELDS = (
    "Ref_Key,"
    "DeletionMark,"
    "Parent_Key,"
    "IsFolder,"
    "Code,"
    "Description,"
    "ЕдиницаИзмерения_Key,"
    "ВидНоменклатуры_Key"
)


def _get_cfg():
    return {
        "base_url": Variable.get("odata_1c_base_url"),
        "username": Variable.get("odata_1c_username"),
        "password": Variable.get("odata_1c_password"),
        "page_size": int(
            Variable.get("odata_1c_page_size", default_var=DEFAULT_PAGE_SIZE)
        ),
        "timeout_sec": int(
            Variable.get("odata_1c_timeout_sec", default_var=120)
        ),
    }


def _norm_text(value):
    return None if value in (None, "", "null") else str(value)


def _session(cfg):
    session = requests.Session()
    session.auth = (cfg["username"], cfg["password"])
    session.headers.update({"Accept": "application/json"})
    return session


def _fetch_all(cfg):
    session = _session(cfg)
    rows = []
    skip = 0

    while True:
        url = (
            f'{cfg["base_url"].rstrip("/")}/{ENTITY}'
            f"?$format=json"
            f"&$select={SELECT_FIELDS}"
            f"&$orderby=Ref_Key"
            f'&$top={cfg["page_size"]}'
            f"&$skip={skip}"
        )

        response = session.get(url, timeout=cfg["timeout_sec"])
        response.raise_for_status()

        batch = response.json().get("value", [])
        if not batch:
            break

        rows.extend(batch)
        logging.info(
            "Получена страница номенклатуры: skip=%s, rows=%s, total=%s",
            skip,
            len(batch),
            len(rows),
        )

        if len(batch) < cfg["page_size"]:
            break

        skip += cfg["page_size"]

    return rows


def _extract_nomenclature(**context):
    cfg = _get_cfg()
    raw = _fetch_all(cfg)

    rows = [
        (
            _norm_text(row.get("Ref_Key")),
            row.get("DeletionMark"),
            _norm_text(row.get("Parent_Key")),
            row.get("IsFolder"),
            _norm_text(row.get("Code")),
            _norm_text(row.get("Description")),
            _norm_text(row.get("ЕдиницаИзмерения_Key")),
            _norm_text(row.get("ВидНоменклатуры_Key")),
        )
        for row in raw
        if row.get("Ref_Key")
    ]

    context["ti"].xcom_push(key="nomenclature_rows", value=rows)
    context["ti"].xcom_push(key="nomenclature_count", value=len(rows))


def _load_nomenclature(**context):
    rows = (
        context["ti"].xcom_pull(
            task_ids="extract_nomenclature",
            key="nomenclature_rows",
        )
        or []
    )

    if not rows:
        raise ValueError("Catalog_Номенклатура вернул 0 строк: загрузка отменена.")

    sql = """
        INSERT INTO raw.r1c_nomenclature_current (
            _id,
            _deletionmark,
            parent_id,
            is_folder,
            code,
            description,
            unit_id,
            nomenclature_type_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (_id) DO UPDATE
        SET
            _deletionmark = EXCLUDED._deletionmark,
            parent_id = EXCLUDED.parent_id,
            is_folder = EXCLUDED.is_folder,
            code = EXCLUDED.code,
            description = EXCLUDED.description,
            unit_id = EXCLUDED.unit_id,
            nomenclature_type_id = EXCLUDED.nomenclature_type_id,
            _loaded_at = now()
    """

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cursor = conn.cursor()

    try:
        cursor.executemany(sql, rows)
        conn.commit()
        logging.info("Загружено/обновлено строк номенклатуры: %s", len(rows))
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def _quality_check(**context):
    count = (
        context["ti"].xcom_pull(
            task_ids="extract_nomenclature",
            key="nomenclature_count",
        )
        or 0
    )

    if count == 0:
        raise ValueError("Quality check failed: Catalog_Номенклатура вернул 0 строк.")

    logging.info("Quality check passed: получено строк номенклатуры: %s", count)


default_args = {
    "owner": "bi",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Полная выгрузка Catalog_Номенклатура из 1С OData в raw-слой",
    start_date=datetime(2026, 9, 23),
    schedule_interval="30 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["1c", "odata", "raw", "catalogs", "nomenclature"],
) as dag:
    extract_nomenclature = PythonOperator(
        task_id="extract_nomenclature",
        python_callable=_extract_nomenclature,
        provide_context=True,
    )

    load_nomenclature = PythonOperator(
        task_id="load_nomenclature",
        python_callable=_load_nomenclature,
        provide_context=True,
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=_quality_check,
        provide_context=True,
    )

    extract_nomenclature >> load_nomenclature >> quality_check
