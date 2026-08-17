from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_extract_tok_vzveshivanie"
POSTGRES_CONN_ID = "postgres_dwh"
DEFAULT_PAGE_SIZE = 1000

# ВНИМАНИЕ: имя OData-сущности ниже — предположение по конвенции проекта.
# $metadata недоступен (HTTP 500), см. SESSION_2026-08-15_BI_1C_AUDIT_AND_ODATA.md.
# Перед боевым запуском проверить вручную: GET {base_url}/Document_АпкРегистрацияВзвешиванияНаТоку?$format=json&$top=1
ENTITY_NAME = "Document_АпкРегистрацияВзвешиванияНаТоку"


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


def _extract_tok(**context):
    cfg = _get_cfg()
    raw = _fetch_all(
        cfg, ENTITY_NAME,
        "Ref_Key,DeletionMark,Posted,Number,Date,ВидВзвешивания,"
        "Отправитель,Получатель,Номенклатура,Водитель,Автомобиль,"
        "ВесТары,ВесБрутто,ВесНетто,Весовщик",
    )
    rows = []
    for d in raw:
        rows.append((
            d.get("Ref_Key"), d.get("DeletionMark"), d.get("Posted"),
            _norm_text(d.get("Number")), d.get("Date"),
            _norm_text(d.get("ВидВзвешивания")),
            _norm_text(d.get("Отправитель")),
            _norm_text(d.get("Получатель")),
            _norm_text(d.get("Номенклатура")),
            _norm_text(d.get("Водитель")),
            _norm_text(d.get("Автомобиль")),
            _safe_float(d.get("ВесТары")),
            _safe_float(d.get("ВесБрутто")),
            _safe_float(d.get("ВесНетто")),
            _norm_text(d.get("Весовщик")),
        ))
    context["ti"].xcom_push(key="tok_rows", value=rows)
    context["ti"].xcom_push(key="tok_count", value=len(rows))


def _load_tok(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_tok", key="tok_rows") or []
    if not rows:
        logging.info("No tok_vzveshivanie rows")
        return
    sql = """
    INSERT INTO raw.r1c_tok_vzveshivanie (
        _id,_deletionmark,_posted,doc_number,doc_date,vid_vzveshivaniya,
        otkuda_text,kuda_text,nomenklatura_text,voditel_id,avtomobil_id,
        ves_tary_kg,ves_brutto_kg,ves_netto_kg,vesovshik_id
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (_id) DO UPDATE SET
        _deletionmark=EXCLUDED._deletionmark,_posted=EXCLUDED._posted,
        doc_number=EXCLUDED.doc_number,doc_date=EXCLUDED.doc_date,
        vid_vzveshivaniya=EXCLUDED.vid_vzveshivaniya,
        otkuda_text=EXCLUDED.otkuda_text,kuda_text=EXCLUDED.kuda_text,
        nomenklatura_text=EXCLUDED.nomenklatura_text,
        voditel_id=EXCLUDED.voditel_id,avtomobil_id=EXCLUDED.avtomobil_id,
        ves_tary_kg=EXCLUDED.ves_tary_kg,ves_brutto_kg=EXCLUDED.ves_brutto_kg,
        ves_netto_kg=EXCLUDED.ves_netto_kg,vesovshik_id=EXCLUDED.vesovshik_id,
        _loaded_at=now()
    """
    conn = pg.get_conn(); cur = conn.cursor()
    cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()


def _quality_check(**context):
    count = context["ti"].xcom_pull(task_ids="extract_tok", key="tok_count") or 0
    if count == 0:
        logging.warning("РегистрацияВзвешиванийНаТоку вернула 0 строк")
    logging.info("Quality check tok_vzveshivanie: %s строк", count)


default_args = {"owner": "bi", "depends_on_past": False, "retries": 2, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id=DAG_ID, default_args=default_args,
    description="Выгрузка Регистрации взвешиваний на току — первичный источник урожая (fact_harvest)",
    start_date=datetime(2026, 8, 17), schedule_interval="30 1 * * *",
    catchup=False, max_active_runs=1, tags=["1c", "odata", "raw", "harvest"],
) as dag:
    t_verify = PythonOperator(task_id="verify_entity", python_callable=_verify_entity, provide_context=True)
    t_extract = PythonOperator(task_id="extract_tok", python_callable=_extract_tok, provide_context=True)
    t_load = PythonOperator(task_id="load_tok", python_callable=_load_tok, provide_context=True)
    t_qc = PythonOperator(task_id="quality_check", python_callable=_quality_check, provide_context=True)
    t_verify >> t_extract >> t_load >> t_qc
