from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_extract_catalogs"
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
def _safe_decimal(v):
    return None if v in (None, "", "null") else v
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


def _extract_upakovki(**context):
    cfg = _get_cfg()
    raw = _fetch_all(cfg, "Catalog_УпаковкиЕдиницыИзмерения", "Ref_Key,DeletionMark,Description,Code,Вес,Объем")
    rows = [(r.get("Ref_Key"), r.get("DeletionMark"), _norm_text(r.get("Description")), _norm_text(r.get("Code")), _safe_decimal(r.get("Вес")), _safe_decimal(r.get("Объем"))) for r in raw]
    context["ti"].xcom_push(key="upakovki_rows", value=rows)
    context["ti"].xcom_push(key="upakovki_count", value=len(rows))

def _extract_sh_tehnika(**context):
    # ВАЖНО: реальная схема Catalog_АпкМоделиСХТехники НЕ содержит полей
    # Производитель / КлассТехники / Мощность. Мощность двигателя хранится
    # в поле МощностьДвигателя. Производитель/класс техники отсутствуют
    # как отдельные простые поля в этой конфигурации.
    cfg = _get_cfg()
    raw = _fetch_all(cfg, "Catalog_АпкМоделиСХТехники", "Ref_Key,DeletionMark,Description,МощностьДвигателя")
    rows = [
        (
            r.get("Ref_Key"),
            r.get("DeletionMark"),
            _norm_text(r.get("Description")),
            None,  # manufacturer — поля "Производитель" нет в этой схеме
            None,  # equipment_class — поля "КлассТехники" нет в этой схеме
            _safe_decimal(r.get("МощностьДвигателя")),
        )
        for r in raw
    ]
    context["ti"].xcom_push(key="sh_tehnika_rows", value=rows)
    context["ti"].xcom_push(key="sh_tehnika_count", value=len(rows))

def _extract_polya(**context):
    # ВАЖНО: реальная схема Catalog_АпкПоля отличается от изначально предполагавшейся.
    # У объекта НЕТ простых полей АпкПлощадьПоляГа / КадастровыйНомер / Хозяйство_Key.
    # Площадь хранится в табличных частях ИсторияПоля (по годам) и КадастровыеУчастки (по участкам).
    # Организация доступна как простое поле Организация_Key.
    cfg = _get_cfg()
    raw = _fetch_all(cfg, "Catalog_АпкПоля", "Ref_Key,DeletionMark,Description,Parent_Key,Организация_Key")
    rows = [
        (
            r.get("Ref_Key"),
            r.get("DeletionMark"),
            _norm_text(r.get("Description")),
            _norm_text(r.get("Parent_Key")),
            None,  # area_ha — требует $expand=ИсторияПоля, вынесено в отдельную доработку
            None,  # cadastr_num — требует $expand=КадастровыеУчастки, вынесено в отдельную доработку
            _norm_text(r.get("Организация_Key")),  # переиспользуем колонку farm_id под организацию
        )
        for r in raw
    ]
    context["ti"].xcom_push(key="polya_rows", value=rows)
    context["ti"].xcom_push(key="polya_count", value=len(rows))


def _load_upakovki(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_upakovki", key="upakovki_rows") or []
    if not rows: logging.info("No upakovki rows"); return
    sql = "INSERT INTO raw.r1c_upakovki (_id,_deletionmark,description,code,weight_kg,volume_l) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (_id) DO UPDATE SET _deletionmark=EXCLUDED._deletionmark,description=EXCLUDED.description,code=EXCLUDED.code,weight_kg=EXCLUDED.weight_kg,volume_l=EXCLUDED.volume_l,_loaded_at=now()"
    conn = pg.get_conn(); cur = conn.cursor(); cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()

def _load_sh_tehnika(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_sh_tehnika", key="sh_tehnika_rows") or []
    if not rows: logging.info("No sh_tehnika rows"); return
    sql = "INSERT INTO raw.r1c_sh_tehnika (_id,_deletionmark,description,manufacturer,equipment_class,power_hp) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (_id) DO UPDATE SET _deletionmark=EXCLUDED._deletionmark,description=EXCLUDED.description,manufacturer=EXCLUDED.manufacturer,equipment_class=EXCLUDED.equipment_class,power_hp=EXCLUDED.power_hp,_loaded_at=now()"
    conn = pg.get_conn(); cur = conn.cursor(); cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()

def _load_polya(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_polya", key="polya_rows") or []
    if not rows: logging.info("No polya rows"); return
    sql = "INSERT INTO raw.r1c_polya (_id,_deletionmark,description,parent_id,area_ha,cadastr_num,farm_id) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (_id) DO UPDATE SET _deletionmark=EXCLUDED._deletionmark,description=EXCLUDED.description,parent_id=EXCLUDED.parent_id,area_ha=EXCLUDED.area_ha,cadastr_num=EXCLUDED.cadastr_num,farm_id=EXCLUDED.farm_id,_loaded_at=now()"
    conn = pg.get_conn(); cur = conn.cursor(); cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()


def _extract_crops(**context):
    cfg = _get_cfg()
    raw = _fetch_all(cfg, "Catalog_АпкКультуры", "Ref_Key,DeletionMark,Description,Parent_Key,IsFolder,КультураПоКлассификатору")
    rows = [
        (r.get("Ref_Key"), r.get("DeletionMark"), _norm_text(r.get("Description")),
         _norm_text(r.get("Parent_Key")), _norm_text(r.get("КультураПоКлассификатору")))
        for r in raw if not r.get("IsFolder")
    ]
    context["ti"].xcom_push(key="crops_rows", value=rows)
    context["ti"].xcom_push(key="crops_count", value=len(rows))

def _extract_fuel_cards(**context):
    cfg = _get_cfg()
    raw = _fetch_all(cfg, "Catalog_АпкТопливныеКарты", "Ref_Key,DeletionMark,Code,Description,Держатель_Key")
    rows = [
        (r.get("Ref_Key"), r.get("DeletionMark"), _norm_text(r.get("Code")),
         _norm_text(r.get("Description")), _norm_text(r.get("Держатель_Key")))
        for r in raw
    ]
    context["ti"].xcom_push(key="fuel_cards_rows", value=rows)
    context["ti"].xcom_push(key="fuel_cards_count", value=len(rows))


def _load_crops(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_crops", key="crops_rows") or []
    if not rows: logging.info("No crops rows"); return
    sql = "INSERT INTO raw.r1c_crops (_id,_deletionmark,description,parent_id,crop_kind) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (_id) DO UPDATE SET _deletionmark=EXCLUDED._deletionmark,description=EXCLUDED.description,parent_id=EXCLUDED.parent_id,crop_kind=EXCLUDED.crop_kind,_loaded_at=now()"
    conn = pg.get_conn(); cur = conn.cursor(); cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()

def _load_fuel_cards(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = context["ti"].xcom_pull(task_ids="extract_fuel_cards", key="fuel_cards_rows") or []
    if not rows: logging.info("No fuel_cards rows"); return
    sql = "INSERT INTO raw.r1c_fuel_cards (_id,_deletionmark,description,card_number,owner_id) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (_id) DO UPDATE SET _deletionmark=EXCLUDED._deletionmark,description=EXCLUDED.description,card_number=EXCLUDED.card_number,owner_id=EXCLUDED.owner_id,_loaded_at=now()"
    conn = pg.get_conn(); cur = conn.cursor(); cur.executemany(sql, rows); conn.commit(); cur.close(); conn.close()


def _quality_check(**context):
    counts = {
        "upakovki": context["ti"].xcom_pull(task_ids="extract_upakovki", key="upakovki_count") or 0,
        "sh_tehnika": context["ti"].xcom_pull(task_ids="extract_sh_tehnika", key="sh_tehnika_count") or 0,
        "polya": context["ti"].xcom_pull(task_ids="extract_polya", key="polya_count") or 0,
        "crops": context["ti"].xcom_pull(task_ids="extract_crops", key="crops_count") or 0,
        "fuel_cards": context["ti"].xcom_pull(task_ids="extract_fuel_cards", key="fuel_cards_count") or 0,
    }
    for name, count in counts.items():
        if count == 0: logging.warning("Справочник %s вернул 0 строк", name)
    logging.info("Quality check: %s", counts)


default_args = {"owner": "bi", "depends_on_past": False, "retries": 2, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id=DAG_ID, default_args=default_args,
    description="Полная выгрузка справочников: Упаковки, СХТехника, Поля из 1С OData",
    start_date=datetime(2026, 7, 1), schedule_interval="0 1 * * *",
    catchup=False, max_active_runs=1, tags=["1c", "odata", "raw", "catalogs"],
) as dag:
    t_ex_up = PythonOperator(task_id="extract_upakovki", python_callable=_extract_upakovki, provide_context=True)
    t_ex_sh = PythonOperator(task_id="extract_sh_tehnika", python_callable=_extract_sh_tehnika, provide_context=True)
    t_ex_po = PythonOperator(task_id="extract_polya", python_callable=_extract_polya, provide_context=True)
    t_ex_cr = PythonOperator(task_id="extract_crops", python_callable=_extract_crops, provide_context=True)
    t_ex_fc = PythonOperator(task_id="extract_fuel_cards", python_callable=_extract_fuel_cards, provide_context=True)
    t_ld_up = PythonOperator(task_id="load_upakovki", python_callable=_load_upakovki, provide_context=True)
    t_ld_sh = PythonOperator(task_id="load_sh_tehnika", python_callable=_load_sh_tehnika, provide_context=True)
    t_ld_po = PythonOperator(task_id="load_polya", python_callable=_load_polya, provide_context=True)
    t_ld_cr = PythonOperator(task_id="load_crops", python_callable=_load_crops, provide_context=True)
    t_ld_fc = PythonOperator(task_id="load_fuel_cards", python_callable=_load_fuel_cards, provide_context=True)
    t_qc = PythonOperator(task_id="quality_check", python_callable=_quality_check, provide_context=True)
    t_ex_up >> t_ld_up >> t_qc
    t_ex_sh >> t_ld_sh >> t_qc
    t_ex_po >> t_ld_po >> t_qc
    t_ex_cr >> t_ld_cr >> t_qc
    t_ex_fc >> t_ld_fc >> t_qc
