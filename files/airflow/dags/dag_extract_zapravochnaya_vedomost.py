from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_extract_zapravochnaya_vedomost"
POSTGRES_CONN_ID = "postgres_dwh"
DEFAULT_PAGE_SIZE = 1000

# Подтверждено прямым запросом OData 2026-09-07 — объект существует и отдаёт данные.
ENTITY_NAME = "Document_АпкЗаправочныеВедомости"

# ВАЖНО: табличная часть ГСМ должна входить в $select как обычное имя поля.
# $expand=ГСМ для табличных частей в этом OData-сервисе не поддерживается —
# реальный запуск с $expand вернул HTTP 501 Not Implemented (проверено curl'ом 2026-09-07).
# С $select, включающим "ГСМ", табличная часть приходит инлайн без ошибок.
DOC_SELECT = (
    "Ref_Key,DeletionMark,Posted,Number,Date,Организация_Key,Подразделение_Key,"
    "Склад_Key,ТранспортноеСредство_Key,Автор_Key,Сотрудник,Комментарий,"
    "ВыдачаСТопливозаправщика,НаемноеТС,Партнер_Key,Соглашение_Key,ГСМ"
)


def _get_cfg():
    return {
        "base_url": Variable.get("odata_1c_base_url"),
        "username": Variable.get("odata_1c_username"),
        "password": Variable.get("odata_1c_password"),
        "page_size": int(Variable.get("odata_1c_page_size", default_var=DEFAULT_PAGE_SIZE)),
        "timeout_sec": int(Variable.get("odata_1c_timeout_sec", default_var=120)),
    }


def _norm_text(v):
    return None if v in (None, "", "null", "00000000-0000-0000-0000-000000000000") else str(v)


def _safe_float(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _extract_time(v):
    """1С отдаёт время как ISO-datetime с фиктивной датой (например
    '0001-01-01T07:00:00'). Postgres-колонка имеет тип time, поэтому
    оставляем только часть после 'T'. Без 'T' — значит уже время/пусто."""
    if not v:
        return None
    s = str(v)
    return s.split("T", 1)[1] if "T" in s else s


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


def _extract_zapravki(**context):
    cfg = _get_cfg()
    raw = _fetch_all(cfg, ENTITY_NAME, DOC_SELECT)
    docs, lines = [], []
    for d in raw:
        doc_id = d.get("Ref_Key")
        docs.append((
            doc_id, d.get("DeletionMark"), d.get("Posted"),
            _norm_text(d.get("Number")), d.get("Date"),
            _norm_text(d.get("Организация_Key")),
            _norm_text(d.get("Подразделение_Key")),
            _norm_text(d.get("Склад_Key")),
            _norm_text(d.get("ТранспортноеСредство_Key")),
            _norm_text(d.get("Автор_Key")),
            _norm_text(d.get("Сотрудник")),
            d.get("Комментарий"),
            d.get("ВыдачаСТоплиозаправщика"),
            d.get("НаемноеТС"),
            _norm_text(d.get("Партнер_Key")),
            _norm_text(d.get("Соглашение_Key")),
        ))
        for row in d.get("ГСМ", []):
            lines.append((
                doc_id, int(row.get("LineNumber") or 0),
                row.get("Дата"),
                _extract_time(row.get("Время")),
                _norm_text(row.get("МаркаТоплива_Key")),
                _safe_float(row.get("Количество")),
                _norm_text(row.get("ТранспортноеСредство_Key")),
                _norm_text(row.get("Сотрудник")),
            ))
    context["ti"].xcom_push(key="zapr_docs", value=docs)
    context["ti"].xcom_push(key="zapr_lines", value=lines)
    context["ti"].xcom_push(key="zapr_count", value=len(docs))
    context["ti"].xcom_push(key="zapr_lines_count", value=len(lines))


def _load_zapr_docs(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_zapravki", key="zapr_docs") or []
    if not rows:
        logging.info("No zapravochnaya_vedomost docs")
        return
    sql = """
    INSERT INTO raw.r1c_zapravochnaya_vedomost (
        _id,_deletionmark,_posted,doc_number,doc_date,
        organizaciya_id,podrazdelenie_id,sklad_id,tehnika_id,avtor_id,
        sotrudnik_id,kommentariy,vydacha_s_toplivozapravschika,naemnoe_ts,
        partner_id,soglashenie_id
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (_id) DO UPDATE SET
        _deletionmark=EXCLUDED._deletionmark,_posted=EXCLUDED._posted,
        doc_number=EXCLUDED.doc_number,doc_date=EXCLUDED.doc_date,
        organizaciya_id=EXCLUDED.organizaciya_id,
        podrazdelenie_id=EXCLUDED.podrazdelenie_id,
        sklad_id=EXCLUDED.sklad_id,tehnika_id=EXCLUDED.tehnika_id,
        avtor_id=EXCLUDED.avtor_id,sotrudnik_id=EXCLUDED.sotrudnik_id,
        kommentariy=EXCLUDED.kommentariy,
        vydacha_s_toplivozapravschika=EXCLUDED.vydacha_s_toplivozapravschika,
        naemnoe_ts=EXCLUDED.naemnoe_ts,partner_id=EXCLUDED.partner_id,
        soglashenie_id=EXCLUDED.soglashenie_id,_loaded_at=now()
    """
    conn = pg.get_conn(); cur = conn.cursor()
    cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()


def _load_zapr_lines(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_zapravki", key="zapr_lines") or []
    if not rows:
        logging.info("No zapravochnaya_vedomost_gsm lines")
        return
    sql = """
    INSERT INTO raw.r1c_zapravochnaya_vedomost_gsm (
        doc_id,line_number,line_date,line_time,marka_topliva_id,
        kolichestvo,line_tehnika_id,line_sotrudnik_id
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (doc_id,line_number) DO UPDATE SET
        line_date=EXCLUDED.line_date,line_time=EXCLUDED.line_time,
        marka_topliva_id=EXCLUDED.marka_topliva_id,
        kolichestvo=EXCLUDED.kolichestvo,
        line_tehnika_id=EXCLUDED.line_tehnika_id,
        line_sotrudnik_id=EXCLUDED.line_sotrudnik_id,
        _loaded_at=now()
    """
    conn = pg.get_conn(); cur = conn.cursor()
    cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()


def _quality_check(**context):
    docs = context["ti"].xcom_pull(task_ids="extract_zapravki", key="zapr_count") or 0
    lines = context["ti"].xcom_pull(task_ids="extract_zapravki", key="zapr_lines_count") or 0
    if docs == 0:
        logging.warning("АпкЗаправочныеВедомости вернул 0 документов")
    logging.info("Quality check zapravochnaya_vedomost: %s документов, %s строк ГСМ", docs, lines)


default_args = {"owner": "bi", "depends_on_past": False, "retries": 2, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id=DAG_ID, default_args=default_args,
    description="Выгрузка Заправочных ведомостей — исправлен парсинг времени ГСМ (InvalidDatetimeFormat на '0001-01-01T07:00:00')",
    start_date=datetime(2026, 9, 7), schedule_interval="45 1 * * *",
    catchup=False, max_active_runs=1, tags=["1c", "odata", "raw", "fuel"],
) as dag:
    t_extract = PythonOperator(task_id="extract_zapravki", python_callable=_extract_zapravki, provide_context=True)
    t_load_docs = PythonOperator(task_id="load_zapr_docs", python_callable=_load_zapr_docs, provide_context=True)
    t_load_lines = PythonOperator(task_id="load_zapr_lines", python_callable=_load_zapr_lines, provide_context=True)
    t_qc = PythonOperator(task_id="quality_check", python_callable=_quality_check, provide_context=True)
    t_extract >> [t_load_docs, t_load_lines] >> t_qc
