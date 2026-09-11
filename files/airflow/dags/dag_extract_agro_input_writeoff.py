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

DAG_ID = "dag_extract_agro_input_writeoff"
POSTGRES_CONN_ID = "postgres_dwh"
DEFAULT_PAGE_SIZE = 200

ENTITY_NAME = "Document_ДвижениеПродукцииИМатериалов"
DOC_TYPE = "АктНаСписанияСемянУдобренийИЯдов"

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
                "Retryable error on attempt %s/%s for %s (status=%s): %s. "
                "Retrying in %ss",
                attempt, MAX_HTTP_RETRIES, full_url, status, exc, backoff,
            )
            time.sleep(backoff)

    raise last_exc


def _fetch_all_full_documents(cfg, entity, filter_expr):
    session = _session(cfg)
    base_url = f'{cfg["base_url"].rstrip("/")}/{entity}'
    rows, skip = [], 0

    while True:
        odata_params = {
            "$format": "json",
            "$filter": filter_expr,
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
    raw_docs = _fetch_all_full_documents(
        cfg, ENTITY_NAME, f"АпкВидДокумента eq '{DOC_TYPE}'"
    )
    with_lines = sum(1 for d in raw_docs if d.get("Товары"))
    logging.info(
        "Verified %s: %s matching documents, %s with non-empty Товары",
        ENTITY_NAME, len(raw_docs), with_lines,
    )


def _extract_agro_input_writeoffs(**context):
    cfg = _get_cfg()

    raw_docs = _fetch_all_full_documents(
        cfg, ENTITY_NAME, f"АпкВидДокумента eq '{DOC_TYPE}'"
    )

    docs = []
    lines = []

    for document in raw_docs:
        doc_id = _norm_text(document.get("Ref_Key"))
        if not doc_id:
            continue

        docs.append((
            doc_id,
            document.get("DeletionMark"),
            document.get("Posted"),
            _norm_text(document.get("Number")),
            document.get("Date"),
            _norm_text(document.get("АпкВидДокумента")),
            _norm_text(document.get("ХозяйственнаяОперация")),
            _norm_text(document.get("Организация_Key")),
            _norm_text(document.get("Отправитель")),
            _norm_text(document.get("Получатель")),
            _norm_text(document.get("АпкВидРаботы_Key")),
            _norm_text(document.get("АпкОбъектЗатрат_Key")),
        ))

        for row in document.get("Товары", []):
            lines.append((
                doc_id,
                int(row.get("LineNumber") or 0),
                _norm_text(row.get("Номенклатура_Key")),
                _norm_text(row.get("Характеристика_Key")),
                _norm_text(row.get("Серия_Key")),
                _safe_float(row.get("Количество")),
                _safe_float(row.get("КоличествоУпаковок")),
                _safe_float(row.get("АпкПлощадьОбработанная")),
                _safe_float(row.get("АпкРасходНаГа")),
                _norm_text(row.get("ГруппаПродукции_Key")),
                _safe_float(row.get("Цена")),
                _safe_float(row.get("Сумма")),
            ))

    context["ti"].xcom_push(key="agro_docs", value=docs)
    context["ti"].xcom_push(key="agro_lines", value=lines)
    context["ti"].xcom_push(key="agro_docs_count", value=len(docs))
    context["ti"].xcom_push(key="agro_lines_count", value=len(lines))


def _load_agro_docs(**context):
    rows = context["ti"].xcom_pull(task_ids="extract_agro_input_writeoffs", key="agro_docs") or []
    if not rows:
        logging.info("No agro-input writeoff documents to load")
        return

    sql = """
    INSERT INTO raw.r1c_field_material_writeoff_doc (
        _id,_deletionmark,_posted,doc_number,doc_date,apk_vid_dokumenta,
        hozyaystvennaya_operaciya,organizaciya_id,otpravitel_sklad_id,
        poluchatel_sklad_id,apk_vid_raboty_id,apk_obekt_zatrat_id
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (_id) DO UPDATE SET
        _deletionmark=EXCLUDED._deletionmark,
        _posted=EXCLUDED._posted,
        doc_number=EXCLUDED.doc_number,
        doc_date=EXCLUDED.doc_date,
        apk_vid_dokumenta=EXCLUDED.apk_vid_dokumenta,
        hozyaystvennaya_operaciya=EXCLUDED.hozyaystvennaya_operaciya,
        organizaciya_id=EXCLUDED.organizaciya_id,
        otpravitel_sklad_id=EXCLUDED.otpravitel_sklad_id,
        poluchatel_sklad_id=EXCLUDED.poluchatel_sklad_id,
        apk_vid_raboty_id=EXCLUDED.apk_vid_raboty_id,
        apk_obekt_zatrat_id=EXCLUDED.apk_obekt_zatrat_id,
        _loaded_at=now()
    """

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()


def _load_agro_lines(**context):
    rows = context["ti"].xcom_pull(task_ids="extract_agro_input_writeoffs", key="agro_lines") or []
    if not rows:
        logging.info("No agro-input writeoff lines to load")
        return

    sql = """
    INSERT INTO raw.r1c_field_material_writeoff_lines (
        doc_id,line_number,nomenklatura_id,harakteristika_id,seriya_id,
        kolichestvo,kolichestvo_upakovok,apk_ploshchad_obrabotannaya,
        apk_raskhod_na_ga,gruppa_produkcii_id,cena,summa
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (doc_id,line_number) DO UPDATE SET
        nomenklatura_id=EXCLUDED.nomenklatura_id,
        harakteristika_id=EXCLUDED.harakteristika_id,
        seriya_id=EXCLUDED.seriya_id,
        kolichestvo=EXCLUDED.kolichestvo,
        kolichestvo_upakovok=EXCLUDED.kolichestvo_upakovok,
        apk_ploshchad_obrabotannaya=EXCLUDED.apk_ploshchad_obrabotannaya,
        apk_raskhod_na_ga=EXCLUDED.apk_raskhod_na_ga,
        gruppa_produkcii_id=EXCLUDED.gruppa_produkcii_id,
        cena=EXCLUDED.cena,
        summa=EXCLUDED.summa,
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
    docs = context["ti"].xcom_pull(
        task_ids="extract_agro_input_writeoffs",
        key="agro_docs_count",
    ) or 0
    lines = context["ti"].xcom_pull(
        task_ids="extract_agro_input_writeoffs",
        key="agro_lines_count",
    ) or 0

    logging.info(
        "Agro-input writeoff quality check: %s documents, %s lines",
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
    description="OData: списание семян, удобрений и СЗР на поля",
    start_date=datetime(2026, 9, 8),
    schedule_interval="30 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["1c", "odata", "raw", "agro-inputs", "fertilizer", "pesticide"],
) as dag:
    verify_entity = PythonOperator(
        task_id="verify_entity",
        python_callable=_verify_entity,
    )
    extract_agro_input_writeoffs = PythonOperator(
        task_id="extract_agro_input_writeoffs",
        python_callable=_extract_agro_input_writeoffs,
    )
    load_agro_docs = PythonOperator(
        task_id="load_agro_docs",
        python_callable=_load_agro_docs,
    )
    load_agro_lines = PythonOperator(
        task_id="load_agro_lines",
        python_callable=_load_agro_lines,
    )
    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=_quality_check,
    )

    verify_entity >> extract_agro_input_writeoffs >> [
        load_agro_docs,
        load_agro_lines,
    ] >> quality_check
