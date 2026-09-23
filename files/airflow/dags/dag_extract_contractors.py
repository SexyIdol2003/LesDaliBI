from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from urllib.parse import quote

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_extract_contractors"
POSTGRES_CONN_ID = "postgres_dwh"
DEFAULT_PAGE_SIZE = 200

ENTITY_NAME = "Catalog_Контрагенты"

RETRYABLE_STATUS_CODES = {500, 502, 503, 504}
MAX_HTTP_RETRIES = 4
BASE_BACKOFF_SEC = 2


def _get_cfg():
    return {
        "base_url": Variable.get("odata_1c_base_url"),
        "username": Variable.get("odata_1c_username"),
        "password": Variable.get("odata_1c_password"),
        "page_size": int(Variable.get("odata_1c_page_size", default_var=DEFAULT_PAGE_SIZE)),
        "timeout_sec": int(Variable.get("odata_1c_timeout_sec", default_var=120)),
    }


def _norm_text(value):
    if value in (None, "", "null", "00000000-0000-0000-0000-000000000000"):
        return None
    return str(value)


def _session(cfg):
    session = requests.Session()
    session.auth = (cfg["username"], cfg["password"])
    session.headers.update({"Accept": "application/json"})
    return session


def _build_query_string(odata_params):
    parts = []
    for key, value in odata_params.items():
        encoded_value = quote(str(value), safe="")
        parts.append(f"{key}={encoded_value}")
    return "&".join(parts)


def _get_with_retry(session, base_url, odata_params, timeout_sec):
    query_string = _build_query_string(odata_params)
    full_url = f"{base_url}?{query_string}"
    last_exc = None

    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        try:
            response = session.get(full_url, timeout=timeout_sec)
            if response.status_code in RETRYABLE_STATUS_CODES:
                raise requests.exceptions.HTTPError(
                    f"{response.status_code} retryable server error",
                    response=response,
                )
            response.raise_for_status()
            return response
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError,
                 requests.exceptions.Timeout) as exc:
            last_exc = exc
            status = getattr(getattr(exc, "response", None), "status_code", None)
            is_retryable_http = status in RETRYABLE_STATUS_CODES
            is_network_error = isinstance(
                exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)
            )

            if attempt == MAX_HTTP_RETRIES or not (is_retryable_http or is_network_error):
                raise

            backoff = BASE_BACKOFF_SEC * (2 ** (attempt - 1))
            logging.warning(
                "Retryable error on attempt %s/%s for %s (status=%s): %s. Retrying in %ss",
                attempt, MAX_HTTP_RETRIES, full_url, status, exc, backoff,
            )
            time.sleep(backoff)

    raise last_exc


def _fetch_all(cfg, entity, select):
    session = _session(cfg)
    base_url = f'{cfg["base_url"].rstrip("/")}/{quote(entity)}'
    rows, skip = [], 0

    while True:
        odata_params = {
            "$format": "json",
            "$select": select,
            "$top": cfg["page_size"],
            "$skip": skip,
        }

        response = _get_with_retry(session, base_url, odata_params, cfg["timeout_sec"])

        batch = response.json().get("value", [])
        if not batch:
            break

        rows.extend(batch)
        if len(batch) < cfg["page_size"]:
            break

        skip += cfg["page_size"]

    return rows


def _verify_entity(**_):
    cfg = _get_cfg()
    rows = _fetch_all(cfg, ENTITY_NAME, "Ref_Key,Description")
    logging.info("Verified %s: %s contractors total", ENTITY_NAME, len(rows))


def _extract_contractors(**context):
    cfg = _get_cfg()
    rows = _fetch_all(
        cfg,
        ENTITY_NAME,
        "Ref_Key,DeletionMark,Description,ИНН,КПП,ЮридическоеФизическоеЛицо",
    )

    records = []
    for row in rows:
        ref_key = _norm_text(row.get("Ref_Key"))
        if not ref_key:
            continue
        records.append((
            ref_key,
            row.get("DeletionMark"),
            row.get("Description"),
            _norm_text(row.get("ИНН")),
            _norm_text(row.get("КПП")),
            _norm_text(row.get("ЮридическоеФизическоеЛицо")),
        ))

    context["ti"].xcom_push(key="contractors", value=records)
    context["ti"].xcom_push(key="contractors_count", value=len(records))


def _load_contractors(**context):
    rows = context["ti"].xcom_pull(task_ids="extract_contractors", key="contractors") or []
    if not rows:
        logging.info("No contractors to load")
        return

    sql = """
    INSERT INTO raw.r1c_contractors (
        _id, _deletionmark, description, inn, kpp, legal_or_individual
    )
    VALUES (%s,%s,%s,%s,%s,%s)
    ON CONFLICT (_id) DO UPDATE SET
        _deletionmark = EXCLUDED._deletionmark,
        description = EXCLUDED.description,
        inn = EXCLUDED.inn,
        kpp = EXCLUDED.kpp,
        legal_or_individual = EXCLUDED.legal_or_individual,
        _loaded_at = now()
    """

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()


def _quality_check(**context):
    count = context["ti"].xcom_pull(task_ids="extract_contractors", key="contractors_count") or 0
    logging.info("Contractors quality check: %s rows", count)


default_args = {
    "owner": "bi",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="OData: справочник контрагентов (для классификации закупок по поставщику: ГСМ/СЗР/удобрения)",
    start_date=datetime(2026, 9, 23),
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["1c", "odata", "raw", "nsi", "contractors"],
) as dag:
    verify_entity = PythonOperator(
        task_id="verify_entity",
        python_callable=_verify_entity,
    )
    extract_contractors = PythonOperator(
        task_id="extract_contractors",
        python_callable=_extract_contractors,
    )
    load_contractors = PythonOperator(
        task_id="load_contractors",
        python_callable=_load_contractors,
    )
    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=_quality_check,
    )

    verify_entity >> extract_contractors >> load_contractors >> quality_check
