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

# ВНИМАНИЕ: имя OData-сущности ниже — предположение по конвенции проекта.
# $metadata недоступен (HTTP 500), см. SESSION_2026-08-15_BI_1C_AUDIT_AND_ODATA.md.
# Перед боевым запуском проверить вручную: GET {base_url}/Document_АпкСписаниеТоплива?$format=json&$top=1
ENTITY_NAME = "Document_АпкСписаниеТоплива"


def _get_cfg():
    return {
        "base_url": Variable.get("odata_1c_base_url"),
        "username": Variable.get("odata_1c_username"),
        "password": Variable.get("odata_1c_password"),
        "page_size": int(Variable.get("odata_1c_page_size", default_var=DEFAULT_PAGE_SIZE)),
        "timeout_sec": int(Variable.get("odata_1c_timeout_sec", default_var=120)),
    }


def _norm_text(v):
    return None if v in (None, "", "null") else str(v)


def _safe_float(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _session(cfg):
    s = requests.Session()
    s.auth = (cfg["username"], cfg["password"])
    s.headers.update({"Accept": "application/json"})
    return s


def _fetch_all(cfg, entity, select):
    session = _session(cfg)
    rows, skip = [], 0
    while True:
        url = (
            f'{cfg["base_url"].rstrip("/")}/{entity}?$format=json&$select={select}'
            f'&$top={cfg["page_size"]}&$skip={skip}'
        )
        resp = session.get(url, timeout=cfg["timeout_sec"])
        resp.raise_for_status()
        batch = resp.json().get("value", [])
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < cfg["page_size"]:
            break
        skip += cfg["page_size"]
    return rows


def _verify_entity(**context):
    """Разовая проверка: тянет 1 запись и логирует реальные ключи JSON,
    чтобы свериться с ENTITY_NAME/полями до полноценной загрузки."""
    cfg = _get_cfg()
    session = _session(cfg)
    url = f'{cfg["base_url"].rstrip("/")}/{ENTITY_NAME}?$format=json&$top=1'
    resp = session.get(url, timeout=cfg["timeout_sec"])
    logging.info("Verify %s -> HTTP %s", ENTITY_NAME, resp.status_code)
    if resp.ok:
        value = resp.json().get("value", [])
        if value:
            logging.info("Пример полей документа: %s", sorted(value[0].keys()))
        else:
            logging.warning("Сущность %s вернула 0 строк — проверьте фильтры/права", ENTITY_NAME)


def _extract_fuel_summary(**context):
    cfg = _get_cfg()
    raw = _fetch_all(
        cfg, ENTITY_NAME,
        "Ref_Key,DeletionMark,Number,Date,НачалоПериода,ОкончаниеПериода,"
        "Организация,Подразделение,ГСМ",
    )
    docs, lines = [], []
    for d in raw:
        doc_id = d.get("Ref_Key")
        docs.append((
            doc_id, d.get("DeletionMark"),
            _norm_text(d.get("Number")), d.get("Date"),
            d.get("НачалоПериода"), d.get("ОкончаниеПериода"),
            _norm_text(d.get("Организация")),
            _norm_text(d.get("Подразделение")),
        ))
        for i, row in enumerate(d.get("ГСМ", []), start=1):
            lines.append((
                doc_id, row.get("LineNumber") or i,
                _norm_text(row.get("ТранспортноеСредство")),
                _norm_text(row.get("МаркаТоплива")),
                _safe_float(row.get("НачальныйОстаток")),
                _safe_float(row.get("Заправлено")),
                _safe_float(row.get("ФактическийРасход")),
                _safe_float(row.get("КонечныйОстаток")),
            ))
    context["ti"].xcom_push(key="fuel_docs", value=docs)
    context["ti"].xcom_push(key="fuel_lines", value=lines)
    context["ti"].xcom_push(key="fuel_count", value=len(docs))


def _load_fuel_docs(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_fuel_summary", key="fuel_docs") or []
    if not rows:
        logging.info("No fuel_summary docs")
        return
    sql = """
    INSERT INTO raw.r1c_fuel_summary_writeoff (
        _id,_deletionmark,doc_number,doc_date,period_start,period_end,
        organization_id,department_id
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (_id) DO UPDATE SET
        _deletionmark=EXCLUDED._deletionmark,
        doc_number=EXCLUDED.doc_number,doc_date=EXCLUDED.doc_date,
        period_start=EXCLUDED.period_start,period_end=EXCLUDED.period_end,
        organization_id=EXCLUDED.organization_id,
        department_id=EXCLUDED.department_id,
        _loaded_at=now()
    """
    conn = pg.get_conn(); cur = conn.cursor()
    cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()


def _load_fuel_lines(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_fuel_summary", key="fuel_lines") or []
    if not rows:
        logging.info("No fuel_summary lines")
        return
    sql = """
    INSERT INTO raw.r1c_fuel_summary_writeoff_lines (
        doc_id,line_number,equipment_id,fuel_brand,
        nachalny_ostatok_l,zapravleno_l,fakticheskiy_rashod_l,konechny_ostatok_l
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (doc_id,line_number) DO UPDATE SET
        equipment_id=EXCLUDED.equipment_id,fuel_brand=EXCLUDED.fuel_brand,
        nachalny_ostatok_l=EXCLUDED.nachalny_ostatok_l,
        zapravleno_l=EXCLUDED.zapravleno_l,
        fakticheskiy_rashod_l=EXCLUDED.fakticheskiy_rashod_l,
        konechny_ostatok_l=EXCLUDED.konechny_ostatok_l,
        _loaded_at=now()
    """
    conn = pg.get_conn(); cur = conn.cursor()
    cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()


def _quality_check(**context):
    count = context["ti"].xcom_pull(task_ids="extract_fuel_summary", key="fuel_count") or 0
    if count == 0:
        logging.warning("СписаниеТопливаПоСуммарнойЗаправке вернул 0 строк")
    logging.info("Quality check fuel_summary: %s документов", count)


default_args = {"owner": "bi", "depends_on_past": False, "retries": 2, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id=DAG_ID, default_args=default_args,
    description="Выгрузка Списания топлива по суммарной заправке — расход ГСМ по технике за месяц",
    start_date=datetime(2026, 8, 17), schedule_interval="40 1 * * *",
    catchup=False, max_active_runs=1, tags=["1c", "odata", "raw", "fuel"],
) as dag:
    t_verify = PythonOperator(task_id="verify_entity", python_callable=_verify_entity, provide_context=True)
    t_extract = PythonOperator(task_id="extract_fuel_summary", python_callable=_extract_fuel_summary, provide_context=True)
    t_load_docs = PythonOperator(task_id="load_fuel_docs", python_callable=_load_fuel_docs, provide_context=True)
    t_load_lines = PythonOperator(task_id="load_fuel_lines", python_callable=_load_fuel_lines, provide_context=True)
    t_qc = PythonOperator(task_id="quality_check", python_callable=_quality_check, provide_context=True)
    t_verify >> t_extract >> t_load_docs >> t_load_lines >> t_qc
