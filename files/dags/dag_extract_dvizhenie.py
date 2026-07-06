from __future__ import annotations

import logging
from datetime import datetime, timedelta
from decimal import Decimal

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_extract_dvizhenie"
POSTGRES_CONN_ID = "ldali_postgres"
DEFAULT_PAGE_SIZE = 500
RAW_DOC_TABLE = "raw.r1c_dvizhenie_produkcii"
RAW_LINE_TABLE = "raw.r1c_dvizhenie_produkcii_lines"


def _get_cfg():
    return {
        "base_url": Variable.get("odata_1c_base_url"),
        "username": Variable.get("odata_1c_username"),
        "password": Variable.get("odata_1c_password"),
        "page_size": int(Variable.get("odata_1c_page_size", default_var=DEFAULT_PAGE_SIZE)),
        "lookback_hours": int(Variable.get("dvizhenie_lookback_hours", default_var=48)),
        "timeout_sec": int(Variable.get("odata_1c_timeout_sec", default_var=120)),
    }


def _to_iso_filter(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")

def _safe_decimal(v):
    return None if v in (None, "", "null") else Decimal(str(v))

def _safe_int(v):
    return None if v in (None, "", "null") else int(v)

def _norm_text(v):
    return None if v in (None, "", "null") else str(v)

def _session(cfg):
    s = requests.Session()
    s.auth = (cfg["username"], cfg["password"])
    s.headers.update({"Accept": "application/json"})
    return s


def _build_url(cfg, dt_from, skip=0):
    dt_str = _to_iso_filter(dt_from)
    return (
        f'{cfg["base_url"].rstrip("/")}/Document_ДвижениеПродукцииИМатериалов'
        f"?$format=json&$filter=Date ge datetime'{dt_str}'"
        f"&$select=Ref_Key,DeletionMark,Posted,Number,Date,Операция,Склад_Key,Контрагент_Key,Комментарий"
        f"&$expand=ТоварыТабличнаяЧасть&$top={cfg['page_size']}&$skip={skip}"
    )


def _extract_docs(**context):
    cfg = _get_cfg()
    last_success = context["dag_run"].conf.get("date_from") if context.get("dag_run") else None
    dt_from = datetime.fromisoformat(last_success) if last_success else datetime.utcnow() - timedelta(hours=cfg["lookback_hours"])

    docs, lines, skip = [], [], 0
    session = _session(cfg)

    while True:
        resp = session.get(_build_url(cfg, dt_from, skip), timeout=cfg["timeout_sec"])
        resp.raise_for_status()
        batch = resp.json().get("value", [])
        if not batch:
            break
        for doc in batch:
            doc_id = doc.get("Ref_Key")
            docs.append((
                doc_id, doc.get("DeletionMark"), doc.get("Posted"),
                _norm_text(doc.get("Number")), doc.get("Date"),
                _norm_text(doc.get("Операция")), _norm_text(doc.get("Склад_Key")),
                _norm_text(doc.get("Контрагент_Key")), _norm_text(doc.get("Комментарий")),
            ))
            for row in doc.get("ТоварыТабличнаяЧасть", []):
                ln = _safe_int(row.get("LineNumber"))
                lines.append((
                    f"{doc_id}_{ln}" if ln is not None else f"{doc_id}_{len(lines)+1}",
                    doc_id, ln,
                    _norm_text(row.get("Поле_Key")), _norm_text(row.get("Номенклатура_Key")),
                    _norm_text(row.get("ЕдиницаИзмерения_Key")),
                    _safe_decimal(row.get("Количество")), _safe_decimal(row.get("Сумма")),
                    _norm_text(row.get("Серия")),
                ))
        if len(batch) < cfg["page_size"]:
            break
        skip += cfg["page_size"]

    context["ti"].xcom_push(key="docs_count", value=len(docs))
    context["ti"].xcom_push(key="lines_count", value=len(lines))
    context["ti"].xcom_push(key="docs_payload", value=docs)
    context["ti"].xcom_push(key="lines_payload", value=lines)


def _load_docs(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    docs = context["ti"].xcom_pull(task_ids="extract_dvizhenie", key="docs_payload") or []
    if not docs:
        logging.info("No documents to load")
        return
    sql = f"""
    INSERT INTO {RAW_DOC_TABLE} (_id,_deletionmark,_posted,doc_number,doc_date,operaciya,sklad_id,kontragent_id,kommentariy)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (_id) DO UPDATE SET
        _deletionmark=EXCLUDED._deletionmark,_posted=EXCLUDED._posted,
        doc_number=EXCLUDED.doc_number,doc_date=EXCLUDED.doc_date,
        operaciya=EXCLUDED.operaciya,sklad_id=EXCLUDED.sklad_id,
        kontragent_id=EXCLUDED.kontragent_id,kommentariy=EXCLUDED.kommentariy,_loaded_at=now()
    """
    conn = pg.get_conn(); cur = conn.cursor()
    cur.executemany(sql, docs); conn.commit(); cur.close(); conn.close()


def _load_lines(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    lines = context["ti"].xcom_pull(task_ids="extract_dvizhenie", key="lines_payload") or []
    if not lines:
        logging.info("No lines to load")
        return
    sql = f"""
    INSERT INTO {RAW_LINE_TABLE} (_id,doc_id,line_number,pole_id,nomenklatura_id,edinica_id,kolichestvo,summa,seriya)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (_id) DO UPDATE SET
        doc_id=EXCLUDED.doc_id,line_number=EXCLUDED.line_number,
        pole_id=EXCLUDED.pole_id,nomenklatura_id=EXCLUDED.nomenklatura_id,
        edinica_id=EXCLUDED.edinica_id,kolichestvo=EXCLUDED.kolichestvo,
        summa=EXCLUDED.summa,seriya=EXCLUDED.seriya,_loaded_at=now()
    """
    conn = pg.get_conn(); cur = conn.cursor()
    cur.executemany(sql, lines); conn.commit(); cur.close(); conn.close()


def _quality_check(**context):
    docs_count = context["ti"].xcom_pull(task_ids="extract_dvizhenie", key="docs_count") or 0
    lines_count = context["ti"].xcom_pull(task_ids="extract_dvizhenie", key="lines_count") or 0
    if docs_count > 0 and lines_count == 0:
        raise ValueError("Загружены документы без строк — проверь $expand")
    logging.info("Данные прошли проверку. docs=%s, lines=%s", docs_count, lines_count)


default_args = {"owner": "bi", "depends_on_past": False, "retries": 2, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id=DAG_ID, default_args=default_args,
    description="Инкрементальная выгрузка Document_ДвижениеПродукцииИМатериалов из 1С OData",
    start_date=datetime(2026, 7, 1), schedule_interval="0 */2 * * *",
    catchup=False, max_active_runs=1, tags=["1c", "odata", "raw", "documents"],
) as dag:
    t_extract = PythonOperator(task_id="extract_dvizhenie", python_callable=_extract_docs, provide_context=True)
    t_load_docs = PythonOperator(task_id="load_dvizhenie_docs", python_callable=_load_docs, provide_context=True)
    t_load_lines = PythonOperator(task_id="load_dvizhenie_lines", python_callable=_load_lines, provide_context=True)
    t_qc = PythonOperator(task_id="quality_check", python_callable=_quality_check, provide_context=True)
    t_extract >> [t_load_docs, t_load_lines] >> t_qc
