from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_extract_equipment_repair"
POSTGRES_CONN_ID = "postgres_dwh"
DEFAULT_PAGE_SIZE = 200
ENTITY_NAME = "Document_АпкВыполнениеРемонтаТС"


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

    sample_count = len(response.json().get("value", []))
    logging.info("Verified %s: %s sample document(s)", ENTITY_NAME, sample_count)


def _extract_repairs(**context):
    cfg = _get_cfg()

    raw = _fetch_all(
        cfg,
        ENTITY_NAME,
        "Ref_Key,DeletionMark,Posted,Статус,ТранспортноеСредство_Key,"
        "ОбъектЭксплуатации_Key,ДатаНачалаФактическая,ДатаЗавершенияФактическая,"
        "ЗаказНаРемонт_Key,Ремонты,ВыполненныеРаботы",
    )

    docs, details, works = [], [], []

    for document in raw:
        doc_id = _norm_text(document.get("Ref_Key"))
        if not doc_id:
            continue

        docs.append((
            doc_id,
            document.get("DeletionMark"),
            document.get("Posted"),
            _norm_text(document.get("Статус")),
            _norm_text(document.get("ТранспортноеСредство_Key")),
            _norm_text(document.get("ОбъектЭксплуатации_Key")),
            document.get("ДатаНачалаФактическая"),
            document.get("ДатаЗавершенияФактическая"),
            _norm_text(document.get("ЗаказНаРемонт_Key")),
        ))

        for index, row in enumerate(document.get("Ремонты", []), start=1):
            details.append((
                doc_id,
                int(row.get("LineNumber") or index),
                _norm_text(row.get("Узел_Key")),
                _norm_text(row.get("ВидРемонта_Key")),
                row.get("ОписаниеРемонта"),
                row.get("ПричинаРемонта"),
            ))

        for index, row in enumerate(document.get("ВыполненныеРаботы", []), start=1):
            works.append((
                doc_id,
                int(row.get("LineNumber") or index),
                _norm_text(row.get("ВидРаботы_Key")),
                _safe_float(row.get("Количество")),
                _safe_float(row.get("Расценка")),
                _safe_float(row.get("Часов")),
                _safe_float(row.get("ЧасовПоНорме")),
                _safe_float(row.get("ОсновнаяЗП")),
                _safe_float(row.get("ДополнительнаяЗП")),
                _safe_float(row.get("ИтогоЗП")),
                _norm_text(row.get("Исполнитель")),
            ))

    context["ti"].xcom_push(key="repair_docs", value=docs)
    context["ti"].xcom_push(key="repair_details", value=details)
    context["ti"].xcom_push(key="repair_works", value=works)
    context["ti"].xcom_push(key="repair_docs_count", value=len(docs))
    context["ti"].xcom_push(key="repair_details_count", value=len(details))
    context["ti"].xcom_push(key="repair_works_count", value=len(works))


def _load_repair_docs(**context):
    rows = context["ti"].xcom_pull(task_ids="extract_repairs", key="repair_docs") or []
    if not rows:
        logging.info("No repair documents to load")
        return

    sql = """
    INSERT INTO raw.r1c_equipment_repair (
        _id,_deletionmark,_posted,status,tehnika_id,obekt_ekspluatacii_id,
        data_nachala_fact,data_zaversheniya_fact,zakaz_na_remont_id
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (_id) DO UPDATE SET
        _deletionmark=EXCLUDED._deletionmark,
        _posted=EXCLUDED._posted,
        status=EXCLUDED.status,
        tehnika_id=EXCLUDED.tehnika_id,
        obekt_ekspluatacii_id=EXCLUDED.obekt_ekspluatacii_id,
        data_nachala_fact=EXCLUDED.data_nachala_fact,
        data_zaversheniya_fact=EXCLUDED.data_zaversheniya_fact,
        zakaz_na_remont_id=EXCLUDED.zakaz_na_remont_id,
        _loaded_at=now()
    """

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()


def _load_repair_details(**context):
    rows = context["ti"].xcom_pull(task_ids="extract_repairs", key="repair_details") or []
    if not rows:
        logging.info("No repair detail rows to load")
        return

    sql = """
    INSERT INTO raw.r1c_equipment_repair_details (
        doc_id,line_number,uzel_id,vid_remonta_id,opisanie_remonta,prichina_remonta
    )
    VALUES (%s,%s,%s,%s,%s,%s)
    ON CONFLICT (doc_id,line_number) DO UPDATE SET
        uzel_id=EXCLUDED.uzel_id,
        vid_remonta_id=EXCLUDED.vid_remonta_id,
        opisanie_remonta=EXCLUDED.opisanie_remonta,
        prichina_remonta=EXCLUDED.prichina_remonta,
        _loaded_at=now()
    """

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()


def _load_repair_works(**context):
    rows = context["ti"].xcom_pull(task_ids="extract_repairs", key="repair_works") or []
    if not rows:
        logging.info("No repair work rows to load")
        return

    sql = """
    INSERT INTO raw.r1c_equipment_repair_works (
        doc_id,line_number,vid_raboty_id,kolichestvo,rascenka,chasov,chasov_po_norme,
        osnovnaya_zp,dopolnitelnaya_zp,itogo_zp,ispolnitel_id
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (doc_id,line_number) DO UPDATE SET
        vid_raboty_id=EXCLUDED.vid_raboty_id,
        kolichestvo=EXCLUDED.kolichestvo,
        rascenka=EXCLUDED.rascenka,
        chasov=EXCLUDED.chasov,
        chasov_po_norme=EXCLUDED.chasov_po_norme,
        osnovnaya_zp=EXCLUDED.osnovnaya_zp,
        dopolnitelnaya_zp=EXCLUDED.dopolnitelnaya_zp,
        itogo_zp=EXCLUDED.itogo_zp,
        ispolnitel_id=EXCLUDED.ispolnitel_id,
        _loaded_at=now()
    """

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()


def _quality_check(**context):
    docs = context["ti"].xcom_pull(task_ids="extract_repairs", key="repair_docs_count") or 0
    details = context["ti"].xcom_pull(task_ids="extract_repairs", key="repair_details_count") or 0
    works = context["ti"].xcom_pull(task_ids="extract_repairs", key="repair_works_count") or 0

    logging.info(
        "Repair quality check: %s documents, %s repair details, %s work rows",
        docs,
        details,
        works,
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
    description="OData: выполнение ремонтов ТС, часы и зарплата ремонта по технике",
    start_date=datetime(2026, 9, 8),
    schedule_interval="20 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["1c", "odata", "raw", "repair", "equipment"],
) as dag:
    verify_entity = PythonOperator(
        task_id="verify_entity",
        python_callable=_verify_entity,
    )
    extract_repairs = PythonOperator(
        task_id="extract_repairs",
        python_callable=_extract_repairs,
    )
    load_repair_docs = PythonOperator(
        task_id="load_repair_docs",
        python_callable=_load_repair_docs,
    )
    load_repair_details = PythonOperator(
        task_id="load_repair_details",
        python_callable=_load_repair_details,
    )
    load_repair_works = PythonOperator(
        task_id="load_repair_works",
        python_callable=_load_repair_works,
    )
    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=_quality_check,
    )

    verify_entity >> extract_repairs >> [
        load_repair_docs,
        load_repair_details,
        load_repair_works,
    ] >> quality_check
