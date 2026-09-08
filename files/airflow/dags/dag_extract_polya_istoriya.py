from __future__ import annotations

import logging
from datetime import datetime, timedelta
from decimal import Decimal

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_extract_polya_istoriya"
POSTGRES_CONN_ID = "postgres_dwh"
DEFAULT_PAGE_SIZE = 200

# Сущность подтверждена вручную 2026-09-08: Catalog_АпкПоля отвечает нормально
# (без 404 / "Доступ запрещён", в отличие от состояния на 2026-08-13).
# Площадь хранится НЕ как плоское поле (АпкПлощадьПоляГа не существует),
# а в табличной части "ИсторияПоля" — по годам урожая и культурам.
# См. SESSION_2026-09-08_POLYA_AREA_DISCOVERY.md.
ENTITY_NAME = "Catalog_АпкПоля"


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


def _safe_decimal(v):
    return None if v in (None, "", "null") else Decimal(str(v))


def _safe_int(v):
    return None if v in (None, "", "null") else int(v)


def _session(cfg):
    s = requests.Session()
    s.auth = (cfg["username"], cfg["password"])
    s.headers.update({"Accept": "application/json"})
    return s


def _build_url(cfg, skip=0):
    select = (
        "Ref_Key,DeletionMark,Description,Parent_Key,IsFolder,"
        "Организация_Key,НомерПоляЕФИС,"
        "СкладСемян_Key,СкладУдобрений_Key,СкладСЗР_Key,СкладПродукции_Key,СкладПрочихМатериалов_Key"
    )
    return (
        f'{cfg["base_url"].rstrip("/")}/{ENTITY_NAME}'
        f"?$format=json&$select={select}"
        f"&$expand=ИсторияПоля"
        f"&$top={cfg['page_size']}&$skip={skip}"
    )


def _verify_entity(**context):
    """Разовая проверка: тянет 1 запись с $expand и логирует реальные ключи JSON."""
    cfg = _get_cfg()
    session = _session(cfg)
    url = f'{cfg["base_url"].rstrip("/")}/{ENTITY_NAME}?$format=json&$top=1&$expand=ИсторияПоля'
    resp = session.get(url, timeout=cfg["timeout_sec"])
    logging.info("Verify %s -> HTTP %s", ENTITY_NAME, resp.status_code)
    if resp.ok:
        value = resp.json().get("value", [])
        if value:
            logging.info("Пример полей документа: %s", sorted(value[0].keys()))
            if value[0].get("ИсторияПоля"):
                logging.info("Пример строки истории: %s", sorted(value[0]["ИсторияПоля"][0].keys()))
        else:
            logging.warning("Сущность %s вернула 0 строк — проверьте фильтры/права", ENTITY_NAME)


def _extract_polya(**context):
    cfg = _get_cfg()
    session = _session(cfg)
    fields, history, skip = [], [], 0

    while True:
        resp = session.get(_build_url(cfg, skip), timeout=cfg["timeout_sec"])
        resp.raise_for_status()
        batch = resp.json().get("value", [])
        if not batch:
            break
        for f in batch:
            field_id = f.get("Ref_Key")
            fields.append((
                field_id, f.get("DeletionMark"),
                _norm_text(f.get("Description")), _norm_text(f.get("Parent_Key")),
                _norm_text(f.get("Организация_Key")), _norm_text(f.get("НомерПоляЕФИС")),
                _norm_text(f.get("СкладСемян_Key")), _norm_text(f.get("СкладУдобрений_Key")),
                _norm_text(f.get("СкладСЗР_Key")), _norm_text(f.get("СкладПродукции_Key")),
                _norm_text(f.get("СкладПрочихМатериалов_Key")),
            ))
            for row in f.get("ИсторияПоля", []):
                ln = _safe_int(row.get("LineNumber"))
                history.append((
                    f"{field_id}_{ln}" if ln is not None else f"{field_id}_{len(history)+1}",
                    field_id, ln,
                    _safe_int(row.get("ГодУрожая")),
                    _norm_text(row.get("Культура")),
                    _safe_decimal(row.get("ПлощадьОбщая")),
                    _safe_decimal(row.get("ПлощадьСева")),
                    _safe_decimal(row.get("ПлощадьКонтура")),
                    row.get("НачалоПериодаАктуальности"),
                    row.get("КонецПериодаАктуальности"),
                    _norm_text(row.get("Предшественник_Key")),
                    _norm_text(row.get("ПредшественникПредставление")),
                    _norm_text(row.get("Подразделение_Key")),
                    row.get("НеИспользуется"),
                ))
        if len(batch) < cfg["page_size"]:
            break
        skip += cfg["page_size"]

    context["ti"].xcom_push(key="fields", value=fields)
    context["ti"].xcom_push(key="history", value=history)
    context["ti"].xcom_push(key="fields_count", value=len(fields))
    context["ti"].xcom_push(key="history_count", value=len(history))


def _load_polya(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_polya", key="fields") or []
    if not rows:
        logging.info("No polya rows")
        return
    sql = """
    INSERT INTO raw.r1c_polya (
        _id,_deletionmark,description,parent_id,
        organization_id,efis_number,
        sklad_semyan_id,sklad_udobreniy_id,sklad_szr_id,sklad_produkcii_id,sklad_prochih_id
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (_id) DO UPDATE SET
        _deletionmark=EXCLUDED._deletionmark,
        description=EXCLUDED.description,
        parent_id=EXCLUDED.parent_id,
        organization_id=EXCLUDED.organization_id,
        efis_number=EXCLUDED.efis_number,
        sklad_semyan_id=EXCLUDED.sklad_semyan_id,
        sklad_udobreniy_id=EXCLUDED.sklad_udobreniy_id,
        sklad_szr_id=EXCLUDED.sklad_szr_id,
        sklad_produkcii_id=EXCLUDED.sklad_produkcii_id,
        sklad_prochih_id=EXCLUDED.sklad_prochih_id,
        _loaded_at=now()
    """
    conn = pg.get_conn(); cur = conn.cursor()
    cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()


def _load_history(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_polya", key="history") or []
    if not rows:
        logging.info("No polya history rows")
        return
    sql = """
    INSERT INTO raw.r1c_polya_istoriya (
        _id, pole_id, line_number, god_urozhaya, kultura_id,
        ploshad_obshaya, ploshad_seva, ploshad_kontura,
        nachalo_perioda, konec_perioda,
        predshestvennik_id, predshestvennik_text,
        podrazdelenie_id, ne_ispolzuetsya
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (_id) DO UPDATE SET
        god_urozhaya=EXCLUDED.god_urozhaya,
        kultura_id=EXCLUDED.kultura_id,
        ploshad_obshaya=EXCLUDED.ploshad_obshaya,
        ploshad_seva=EXCLUDED.ploshad_seva,
        ploshad_kontura=EXCLUDED.ploshad_kontura,
        nachalo_perioda=EXCLUDED.nachalo_perioda,
        konec_perioda=EXCLUDED.konec_perioda,
        predshestvennik_id=EXCLUDED.predshestvennik_id,
        predshestvennik_text=EXCLUDED.predshestvennik_text,
        podrazdelenie_id=EXCLUDED.podrazdelenie_id,
        ne_ispolzuetsya=EXCLUDED.ne_ispolzuetsya,
        _loaded_at=now()
    """
    conn = pg.get_conn(); cur = conn.cursor()
    cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()


def _quality_check(**context):
    fields_count = context["ti"].xcom_pull(task_ids="extract_polya", key="fields_count") or 0
    history_count = context["ti"].xcom_pull(task_ids="extract_polya", key="history_count") or 0
    if fields_count > 0 and history_count == 0:
        raise ValueError("Загружены поля без истории площадей — проверь $expand=ИсторияПоля")
    logging.info("Quality check polya: fields=%s, history=%s", fields_count, history_count)


default_args = {"owner": "bi", "depends_on_past": False, "retries": 2, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id=DAG_ID, default_args=default_args,
    description="Полная выгрузка Catalog_АпкПоля + табличная часть ИсторияПоля (площадь по годам/культурам)",
    start_date=datetime(2026, 9, 8), schedule_interval="15 1 * * *",
    catchup=False, max_active_runs=1, tags=["1c", "odata", "raw", "polya", "harvest"],
) as dag:
    t_verify = PythonOperator(task_id="verify_entity", python_callable=_verify_entity, provide_context=True)
    t_extract = PythonOperator(task_id="extract_polya", python_callable=_extract_polya, provide_context=True)
    t_load_polya = PythonOperator(task_id="load_polya", python_callable=_load_polya, provide_context=True)
    t_load_history = PythonOperator(task_id="load_history", python_callable=_load_history, provide_context=True)
    t_qc = PythonOperator(task_id="quality_check", python_callable=_quality_check, provide_context=True)
    t_verify >> t_extract >> t_load_polya >> t_load_history >> t_qc
