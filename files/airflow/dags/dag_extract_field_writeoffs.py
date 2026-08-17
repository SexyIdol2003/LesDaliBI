from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_extract_field_writeoffs"
POSTGRES_CONN_ID = "postgres_dwh"
DEFAULT_PAGE_SIZE = 1000

# ВНИМАНИЕ: имя OData-сущности ниже — предположение по конвенции проекта.
# $metadata недоступен (HTTP 500), см. SESSION_2026-08-15_BI_1C_AUDIT_AND_ODATA.md.
# Перед боевым запуском проверить вручную: GET {base_url}/Document_АпкСписаниеСемянУдобренийИЯдов?$format=json&$top=1
ENTITY_NAME = "Document_АпкСписаниеСемянУдобренийИЯдов"

# ИСПРАВЛЕНО (см. SESSION_2026-08-17): реальная схема raw.r1c_field_writeoff_doc/lines
# использует doc_ref (не _id) и line_no (не line_number) — унаследовано от 02_raw.sql (2026-06-23).


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


def _extract_writeoffs(**context):
    cfg = _get_cfg()
    raw = _fetch_all(
        cfg, ENTITY_NAME,
        "Ref_Key,DeletionMark,Posted,Number,Date,Отправитель,Получатель,"
        "ХозяйственнаяОперация,Ответственный,Материалы",
    )
    docs, lines = [], []
    for d in raw:
        doc_ref = d.get("Ref_Key")
        docs.append((
            doc_ref, d.get("DeletionMark"), d.get("Posted"),
            _norm_text(d.get("Number")), d.get("Date"),
            _norm_text(d.get("Отправитель")),
            _norm_text(d.get("Получатель")),
            _norm_text(d.get("ХозяйственнаяОперация")),
            _norm_text(d.get("Ответственный")),
        ))
        for row in d.get("Материалы", []):
            lines.append((
                doc_ref, row.get("LineNumber"),
                _norm_text(row.get("Номенклатура_Key")),
                _safe_float(row.get("Количество")),
                _norm_text(row.get("ЕдиницаИзмерения_Key")),
            ))
    context["ti"].xcom_push(key="writeoff_docs", value=docs)
    context["ti"].xcom_push(key="writeoff_lines", value=lines)
    context["ti"].xcom_push(key="writeoff_count", value=len(docs))


def _load_writeoff_docs(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_writeoffs", key="writeoff_docs") or []
    if not rows:
        logging.info("No field_writeoff docs")
        return
    sql = """
    INSERT INTO raw.r1c_field_writeoff_doc (
        doc_ref,_deletionmark,_posted,doc_number,doc_date,
        otpravitel_text,poluchatel_text,operaciya,otvetstvennyi_id
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (doc_ref) DO UPDATE SET
        _deletionmark=EXCLUDED._deletionmark,_posted=EXCLUDED._posted,
        doc_number=EXCLUDED.doc_number,doc_date=EXCLUDED.doc_date,
        otpravitel_text=EXCLUDED.otpravitel_text,
        poluchatel_text=EXCLUDED.poluchatel_text,
        operaciya=EXCLUDED.operaciya,
        otvetstvennyi_id=EXCLUDED.otvetstvennyi_id,
        _loaded_at=now()
    """
    conn = pg.get_conn(); cur = conn.cursor()
    cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()


def _load_writeoff_lines(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_writeoffs", key="writeoff_lines") or []
    if not rows:
        logging.info("No field_writeoff lines")
        return
    sql = """
    INSERT INTO raw.r1c_field_writeoff_lines (doc_ref,line_no,nomenklatura_ref,quantity,uom_ref)
    VALUES (%s,%s,%s,%s,%s)
    ON CONFLICT (doc_ref,line_no) DO UPDATE SET
        nomenklatura_ref=EXCLUDED.nomenklatura_ref,
        quantity=EXCLUDED.quantity,
        uom_ref=EXCLUDED.uom_ref,
        _loaded_at=now()
    """
    conn = pg.get_conn(); cur = conn.cursor()
    cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()


def _quality_check(**context):
    count = context["ti"].xcom_pull(task_ids="extract_writeoffs", key="writeoff_count") or 0
    if count == 0:
        logging.warning("АктСписанияСеменУдобренийИЯдов вернул 0 строк")
    logging.info("Quality check field_writeoffs: %s документов", count)


default_args = {"owner": "bi", "depends_on_past": False, "retries": 2, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id=DAG_ID, default_args=default_args,
    description="Выгрузка Актов на списание семян, удобрений и ядов — материальные затраты по полю",
    start_date=datetime(2026, 8, 17), schedule_interval="35 1 * * *",
    catchup=False, max_active_runs=1, tags=["1c", "odata", "raw", "field_costs"],
) as dag:
    t_verify = PythonOperator(task_id="verify_entity", python_callable=_verify_entity, provide_context=True)
    t_extract = PythonOperator(task_id="extract_writeoffs", python_callable=_extract_writeoffs, provide_context=True)
    t_load_docs = PythonOperator(task_id="load_writeoff_docs", python_callable=_load_writeoff_docs, provide_context=True)
    t_load_lines = PythonOperator(task_id="load_writeoff_lines", python_callable=_load_writeoff_lines, provide_context=True)
    t_qc = PythonOperator(task_id="quality_check", python_callable=_quality_check, provide_context=True)
    t_verify >> t_extract >> t_load_docs >> t_load_lines >> t_qc
