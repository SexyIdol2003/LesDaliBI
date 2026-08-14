from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_extract_catalogs2"
POSTGRES_CONN_ID = "postgres_dwh"
DEFAULT_PAGE_SIZE = 1000


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

def _session(cfg):
    s = requests.Session()
    s.auth = (cfg["username"], cfg["password"])
    s.headers.update({"Accept": "application/json"})
    return s

def _fetch_all(cfg, entity, select):
    session = _session(cfg)
    rows, skip = [], 0
    while True:
        url = f'{cfg["base_url"].rstrip("/")}/{entity}?$format=json&$select={select}&$top={cfg["page_size"]}&$skip={skip}'
        resp = session.get(url, timeout=cfg["timeout_sec"])
        resp.raise_for_status()
        batch = resp.json().get("value", [])
        if not batch: break
        rows.extend(batch)
        if len(batch) < cfg["page_size"]: break
        skip += cfg["page_size"]
    return rows


def _extract_employees(**context):
    cfg = _get_cfg()
    raw = _fetch_all(cfg, "Catalog_Сотрудники", "Ref_Key,DeletionMark,Description,ФизическоеЛицо_Key")
    rows = [
        (r.get("Ref_Key"), r.get("DeletionMark"), _norm_text(r.get("Description")),
         _norm_text(r.get("ФизическоеЛицо_Key")), None, None)
        for r in raw
    ]
    context["ti"].xcom_push(key="employees_rows", value=rows)
    context["ti"].xcom_push(key="employees_count", value=len(rows))

def _extract_cashflow_items(**context):
    cfg = _get_cfg()
    raw = _fetch_all(cfg, "Catalog_СтатьиДвиженияДенежныхСредств", "Ref_Key,DeletionMark,Description,Parent_Key,ВидДвиженияДенежныхСредств")
    rows = [
        (r.get("Ref_Key"), r.get("DeletionMark"), _norm_text(r.get("Description")),
         _norm_text(r.get("Parent_Key")), _norm_text(r.get("ВидДвиженияДенежныхСредств")))
        for r in raw
    ]
    context["ti"].xcom_push(key="cashflow_items_rows", value=rows)
    context["ti"].xcom_push(key="cashflow_items_count", value=len(rows))

def _extract_crop_rotations(**context):
    cfg = _get_cfg()
    raw = _fetch_all(cfg, "Catalog_АпкСевообороты", "Ref_Key,DeletionMark,Description")
    rows = [
        (r.get("Ref_Key"), r.get("DeletionMark"), _norm_text(r.get("Description")))
        for r in raw
    ]
    context["ti"].xcom_push(key="crop_rotations_rows", value=rows)
    context["ti"].xcom_push(key="crop_rotations_count", value=len(rows))

def _extract_company_structure(**context):
    cfg = _get_cfg()
    raw = _fetch_all(cfg, "Catalog_СтруктураПредприятия", "Ref_Key,DeletionMark,Description,Parent_Key,АпкТип")
    rows = [
        (r.get("Ref_Key"), r.get("DeletionMark"), _norm_text(r.get("Description")),
         _norm_text(r.get("Parent_Key")), None, _norm_text(r.get("АпкТип")))
        for r in raw
    ]
    context["ti"].xcom_push(key="company_structure_rows", value=rows)
    context["ti"].xcom_push(key="company_structure_count", value=len(rows))


def _load_employees(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_employees", key="employees_rows") or []
    if not rows: logging.info("No employees rows"); return
    sql = "INSERT INTO raw.r1c_employees (_id,_deletionmark,description,individual_id,position_id,department_id) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (_id) DO UPDATE SET _deletionmark=EXCLUDED._deletionmark,description=EXCLUDED.description,individual_id=EXCLUDED.individual_id,_loaded_at=now()"
    conn = pg.get_conn(); cur = conn.cursor(); cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()

def _load_cashflow_items(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_cashflow_items", key="cashflow_items_rows") or []
    if not rows: logging.info("No cashflow_items rows"); return
    sql = "INSERT INTO raw.r1c_cashflow_items (_id,_deletionmark,description,parent_id,cashflow_kind) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (_id) DO UPDATE SET _deletionmark=EXCLUDED._deletionmark,description=EXCLUDED.description,parent_id=EXCLUDED.parent_id,cashflow_kind=EXCLUDED.cashflow_kind,_loaded_at=now()"
    conn = pg.get_conn(); cur = conn.cursor(); cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()

def _load_crop_rotations(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_crop_rotations", key="crop_rotations_rows") or []
    if not rows: logging.info("No crop_rotations rows"); return
    sql = "INSERT INTO raw.r1c_crop_rotations (_id,_deletionmark,description) VALUES (%s,%s,%s) ON CONFLICT (_id) DO UPDATE SET _deletionmark=EXCLUDED._deletionmark,description=EXCLUDED.description,_loaded_at=now()"
    conn = pg.get_conn(); cur = conn.cursor(); cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()

def _load_company_structure(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_company_structure", key="company_structure_rows") or []
    if not rows: logging.info("No company_structure rows"); return
    sql = "INSERT INTO raw.r1c_company_structure (_id,_deletionmark,description,parent_id,inn,node_type) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (_id) DO UPDATE SET _deletionmark=EXCLUDED._deletionmark,description=EXCLUDED.description,parent_id=EXCLUDED.parent_id,node_type=EXCLUDED.node_type,_loaded_at=now()"
    conn = pg.get_conn(); cur = conn.cursor(); cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()


def _extract_equipment(**context):
    cfg = _get_cfg()
    raw = _fetch_all(cfg, "Catalog_ТранспортныеСредства", "Ref_Key,DeletionMark,Description,IsFolder,АпкМодель,АпкХозНомер,АпкТипТС")
    rows = [
        (r.get("Ref_Key"), r.get("DeletionMark"), _norm_text(r.get("Description")),
         _norm_text(r.get("АпкМодель")), _norm_text(r.get("АпкХозНомер")), _norm_text(r.get("АпкТипТС")))
        for r in raw if not r.get("IsFolder")
    ]
    context["ti"].xcom_push(key="equipment_rows", value=rows)
    context["ti"].xcom_push(key="equipment_count", value=len(rows))

def _extract_expense_items(**context):
    cfg = _get_cfg()
    raw = _fetch_all(cfg, "Catalog_СтатьиКалькуляции", "Ref_Key,DeletionMark,Description,Parent_Key,ТипЗатрат,IsFolder")
    rows = [
        (r.get("Ref_Key"), r.get("DeletionMark"), _norm_text(r.get("Description")),
         _norm_text(r.get("Parent_Key")), _norm_text(r.get("ТипЗатрат")))
        for r in raw
    ]
    context["ti"].xcom_push(key="expense_items_rows", value=rows)
    context["ti"].xcom_push(key="expense_items_count", value=len(rows))


def _load_equipment(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_equipment", key="equipment_rows") or []
    if not rows: logging.info("No equipment rows"); return
    sql = "INSERT INTO raw.r1c_equipment (_id,_deletionmark,description,model_id,reg_number,equipment_type) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (_id) DO UPDATE SET _deletionmark=EXCLUDED._deletionmark,description=EXCLUDED.description,model_id=EXCLUDED.model_id,reg_number=EXCLUDED.reg_number,equipment_type=EXCLUDED.equipment_type,_loaded_at=now()"
    conn = pg.get_conn(); cur = conn.cursor(); cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()

def _load_expense_items(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_expense_items", key="expense_items_rows") or []
    if not rows: logging.info("No expense_items rows"); return
    sql = "INSERT INTO raw.r1c_expense_items (_id,_deletionmark,description,parent_id,expense_kind) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (_id) DO UPDATE SET _deletionmark=EXCLUDED._deletionmark,description=EXCLUDED.description,parent_id=EXCLUDED.parent_id,expense_kind=EXCLUDED.expense_kind,_loaded_at=now()"
    conn = pg.get_conn(); cur = conn.cursor(); cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()


def _quality_check(**context):
    counts = {
        "employees": context["ti"].xcom_pull(task_ids="extract_employees", key="employees_count") or 0,
        "cashflow_items": context["ti"].xcom_pull(task_ids="extract_cashflow_items", key="cashflow_items_count") or 0,
        "crop_rotations": context["ti"].xcom_pull(task_ids="extract_crop_rotations", key="crop_rotations_count") or 0,
        "company_structure": context["ti"].xcom_pull(task_ids="extract_company_structure", key="company_structure_count") or 0,
        "equipment": context["ti"].xcom_pull(task_ids="extract_equipment", key="equipment_count") or 0,
        "expense_items": context["ti"].xcom_pull(task_ids="extract_expense_items", key="expense_items_count") or 0,
    }
    for name, count in counts.items():
        if count == 0: logging.warning("Справочник %s вернул 0 строк", name)
    logging.info("Quality check: %s", counts)


default_args = {"owner": "bi", "depends_on_past": False, "retries": 2, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id=DAG_ID, default_args=default_args,
    description="Выгрузка справочников v2: Сотрудники, СтатьиДДС, Севообороты, СтруктураПредприятия",
    start_date=datetime(2026, 7, 1), schedule_interval="15 1 * * *",
    catchup=False, max_active_runs=1, tags=["1c", "odata", "raw", "catalogs"],
) as dag:
    t_ex_em = PythonOperator(task_id="extract_employees", python_callable=_extract_employees, provide_context=True)
    t_ex_cf = PythonOperator(task_id="extract_cashflow_items", python_callable=_extract_cashflow_items, provide_context=True)
    t_ex_cr = PythonOperator(task_id="extract_crop_rotations", python_callable=_extract_crop_rotations, provide_context=True)
    t_ex_cs = PythonOperator(task_id="extract_company_structure", python_callable=_extract_company_structure, provide_context=True)
    t_ex_eq = PythonOperator(task_id="extract_equipment", python_callable=_extract_equipment, provide_context=True)
    t_ex_ei = PythonOperator(task_id="extract_expense_items", python_callable=_extract_expense_items, provide_context=True)
    t_ld_em = PythonOperator(task_id="load_employees", python_callable=_load_employees, provide_context=True)
    t_ld_cf = PythonOperator(task_id="load_cashflow_items", python_callable=_load_cashflow_items, provide_context=True)
    t_ld_cr = PythonOperator(task_id="load_crop_rotations", python_callable=_load_crop_rotations, provide_context=True)
    t_ld_cs = PythonOperator(task_id="load_company_structure", python_callable=_load_company_structure, provide_context=True)
    t_ld_eq = PythonOperator(task_id="load_equipment", python_callable=_load_equipment, provide_context=True)
    t_ld_ei = PythonOperator(task_id="load_expense_items", python_callable=_load_expense_items, provide_context=True)
    t_qc = PythonOperator(task_id="quality_check", python_callable=_quality_check, provide_context=True)
    t_ex_em >> t_ld_em >> t_qc
    t_ex_cf >> t_ld_cf >> t_qc
    t_ex_cr >> t_ld_cr >> t_qc
    t_ex_cs >> t_ld_cs >> t_qc
    t_ex_eq >> t_ld_eq >> t_qc
    t_ex_ei >> t_ld_ei >> t_qc
