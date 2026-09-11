from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_extract_fuel_summary"
POSTGRES_CONN_ID = "postgres_dwh"
DEFAULT_PAGE_SIZE = 1000

ENTITY_NAME = "Document_АпкСписаниеТопливаПоСуммарнойЗаправке"


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


def _safe_float(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _session(cfg):
    session = requests.Session()
    session.auth = (cfg["username"], cfg["password"])
    session.headers.update({"Accept": "application/json"})
    return session


def _fetch_all(cfg, entity, select):
    session = _session(cfg)
    rows, skip = [], 0

    while True:
        url = (
            f'{cfg["base_url"].rstrip("/")}/{entity}?$format=json'
            f'&$select={select}&$top={cfg["page_size"]}&$skip={skip}'
        )
        response = session.get(url, timeout=cfg["timeout_sec"])
        response.raise_for_status()

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
    session = _session(cfg)
    url = f'{cfg["base_url"].rstrip("/")}/{ENTITY_NAME}?$format=json&$top=1'
    response = session.get(url, timeout=cfg["timeout_sec"])
    response.raise_for_status()
    count = len(response.json().get("value", []))
    logging.info("Verified %s: %s sample document(s)", ENTITY_NAME, count)
    if count == 0:
        logging.warning("%s returned 0 documents", ENTITY_NAME)


def _extract_fuel_summary(**context):
    cfg = _get_cfg()

    raw = _fetch_all(
        cfg,
        ENTITY_NAME,
        "Ref_Key,DeletionMark,Posted,Number,Date,НачалоПериода,ОкончаниеПериода,"
        "Организация_Key,Подразделение_Key,ГСМ",
    )

    docs, lines = [], []

    for document in raw:
        doc_id = _norm_text(document.get("Ref_Key"))
        if not doc_id:
            continue

        docs.append((
            doc_id,
            document.get("DeletionMark"),
            document.get("Posted"),
            _norm_text(document.get("Number")),
            document.get("Date"),
            document.get("НачалоПериода"),
            document.get("ОкончаниеПериода"),
            _norm_text(document.get("Организация_Key")),
            _norm_text(document.get("Подразделение_Key")),
        ))

        for index, row in enumerate(document.get("ГСМ", []), start=1):
            lines.append((
                doc_id,
                int(row.get("LineNumber") or index),
                _norm_text(row.get("ТранспортноеСредство_Key")),
                _norm_text(row.get("МаркаТоплива_Key")),
                _safe_float(row.get("НачальныйОстаток")),
                _safe_float(row.get("КонечныйОстаток")),
                _safe_float(row.get("Заправлено")),
                _safe_float(row.get("ФактическийРасход")),
            ))

    context["ti"].xcom_push(key="fuel_docs", value=docs)
    context["ti"].xcom_push(key="fuel_lines", value=lines)
    context["ti"].xcom_push(key="fuel_docs_count", value=len(docs))
    context["ti"].xcom_push(key="fuel_lines_count", value=len(lines))


def _load_fuel_docs(**context):
    rows = context["ti"].xcom_pull(task_ids="extract_fuel_summary", key="fuel_docs") or []
    if not rows:
        logging.info("No fuel writeoff documents to load")
        return

    sql = """
    INSERT INTO raw.r1c_fuel_writeoff_summary (
        _id, _deletionmark, _posted, doc_number, doc_date,
        period_start, period_end, organizaciya_id, podrazdelenie_id
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (_id) DO UPDATE SET
        _deletionmark = EXCLUDED._deletionmark,
        _posted = EXCLUDED._posted,
        doc_number = EXCLUDED.doc_number,
        doc_date = EXCLUDED.doc_date,
        period_start = EXCLUDED.period_start,
        period_end = EXCLUDED.period_end,
        organizaciya_id = EXCLUDED.organizaciya_id,
        podrazdelenie_id = EXCLUDED.podrazdelenie_id,
        _loaded_at = now()
    """

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()


def _load_fuel_lines(**context):
    rows = context["ti"].xcom_pull(task_ids="extract_fuel_summary", key="fuel_lines") or []
    if not rows:
        logging.info("No fuel writeoff lines to load")
        return

    sql = """
    INSERT INTO raw.r1c_fuel_writeoff_summary_gsm (
        doc_id, line_number, tehnika_id, marka_topliva_id,
        nachalny_ostatok, konechny_ostatok, zapravleno, fakticheskiy_raskhod
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (doc_id, line_number) DO UPDATE SET
        tehnika_id = EXCLUDED.tehnika_id,
        marka_topliva_id = EXCLUDED.marka_topliva_id,
        nachalny_ostatok = EXCLUDED.nachalny_ostatok,
        konechny_ostatok = EXCLUDED.konechny_ostatok,
        zapravleno = EXCLUDED.zapravleno,
        fakticheskiy_raskhod = EXCLUDED.fakticheskiy_raskhod,
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
    docs = context["ti"].xcom_pull(task_ids="extract_fuel_summary", key="fuel_docs_count") or 0
    lines = context["ti"].xcom_pull(task_ids="extract_fuel_summary", key="fuel_lines_count") or 0

    if docs == 0:
        logging.warning("%s returned 0 documents", ENTITY_NAME)

    logging.info(
        "Fuel writeoff quality check: %s documents, %s fuel lines",
        docs,
        lines,
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
    description="OData: факт списания топлива по суммарной заправке, по технике и периоду",
    start_date=datetime(2026, 9, 8),
    schedule_interval="10 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["1c", "odata", "raw", "fuel"],
) as dag:
    verify_entity = PythonOperator(
        task_id="verify_entity",
        python_callable=_verify_entity,
    )
    extract_fuel_summary = PythonOperator(
        task_id="extract_fuel_summary",
        python_callable=_extract_fuel_summary,
    )
    load_fuel_docs = PythonOperator(
        task_id="load_fuel_docs",
        python_callable=_load_fuel_docs,
    )
    load_fuel_lines = PythonOperator(
        task_id="load_fuel_lines",
        python_callable=_load_fuel_lines,
    )
    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=_quality_check,
    )

    verify_entity >> extract_fuel_summary >> [load_fuel_docs, load_fuel_lines] >> quality_check
