from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_extract_work_types"
POSTGRES_CONN_ID = "postgres_dwh"
ENTITY_NAME = "Catalog_ВидыРаботСотрудников"
RAW_TABLE = "raw.r1c_tech_operations"
RAW_NORMS_TABLE = "raw.r1c_operation_fuel_norms"
DEFAULT_PAGE_SIZE = 500


def get_cfg():
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


def fetch_work_types(**context):
    """Полная выгрузка справочника без $select: нормы ГСМ (табличная часть АпкНормы)
    приходят инлайн только при полном ответе — подтверждено прямым запросом OData 2026-09-07
    (curl без $select и без $expand вернул все табличные части, включая АпкНормы, полностью).
    Справочник небольшой (123 строки на 2026-09-07), полная выгрузка не создаёт нагрузки."""
    cfg = get_cfg()
    session = requests.Session()
    session.auth = (cfg["username"], cfg["password"])
    session.headers.update({"Accept": "application/json"})

    work_type_rows = []
    norm_rows = []
    skip = 0

    while True:
        url = (
            f'{cfg["base_url"].rstrip("/")}/{ENTITY_NAME}'
            f"?$format=json&$top={cfg['page_size']}&$skip={skip}"
        )
        response = session.get(url, timeout=cfg["timeout_sec"])
        response.raise_for_status()

        batch = response.json().get("value", [])
        if not batch:
            break

        for item in batch:
            operation_id = item.get("Ref_Key")
            work_type_rows.append((
                str(operation_id),
                item.get("DeletionMark"),
                item.get("Description") or None,
                _norm_text(item.get("Parent_Key")),
                item.get("Code") or None,
            ))
            for norm in item.get("АпкНормы", []):
                ln = norm.get("LineNumber")
                if ln is None:
                    continue
                norm_rows.append((
                    str(operation_id),
                    int(ln),
                    _norm_text(norm.get("МодельТехники")),
                    _norm_text(norm.get("МодельОборудования")),
                    _safe_float(norm.get("СменнаяНормаВыработки")),
                    _safe_float(norm.get("НормаРасходаТоплива")),
                    _safe_float(norm.get("ПродолжительностьСмены")),
                    norm.get("ЕдиницаИзмеренияСменнойНормыВыработки") or None,
                    norm.get("БазаРасчетаРасходаГСМ") or None,
                    _norm_text(norm.get("КлючСвязиСтрокиНорм")),
                ))

        if len(batch) < cfg["page_size"]:
            break
        skip += cfg["page_size"]

    logging.info("Fetched %s work types, %s fuel norm rows from %s", len(work_type_rows), len(norm_rows), ENTITY_NAME)
    context["ti"].xcom_push(key="work_type_rows", value=work_type_rows)
    context["ti"].xcom_push(key="work_type_count", value=len(work_type_rows))
    context["ti"].xcom_push(key="norm_rows", value=norm_rows)
    context["ti"].xcom_push(key="norm_count", value=len(norm_rows))


def load_work_types(**context):
    rows = context["ti"].xcom_pull(
        task_ids="extract_work_types",
        key="work_type_rows",
    ) or []

    if not rows:
        raise ValueError("Catalog_ВидыРаботСотрудников returned zero rows")

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = f"""
        INSERT INTO {RAW_TABLE}
            (_id, _deletionmark, description, parent_id, operation_kind)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (_id) DO UPDATE SET
            _deletionmark = EXCLUDED._deletionmark,
            description = EXCLUDED.description,
            parent_id = EXCLUDED.parent_id,
            operation_kind = EXCLUDED.operation_kind,
            _loaded_at = now()
    """

    conn = pg.get_conn()
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()

    logging.info("Loaded %s rows into %s", len(rows), RAW_TABLE)


def load_fuel_norms(**context):
    rows = context["ti"].xcom_pull(
        task_ids="extract_work_types",
        key="norm_rows",
    ) or []

    if not rows:
        logging.warning("No fuel norm rows to load (АпкНормы empty for all fetched work types)")
        return

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = f"""
        INSERT INTO {RAW_NORMS_TABLE} (
            operation_id, line_number, model_tehniki_id, model_oborudovaniya_id,
            smennaya_norma_vyrabotki, norma_rashoda_topliva, prodolzhitelnost_smeny,
            edinica_smennoy_normy, baza_rascheta_gsm, klyuch_svyazi_stroki_norm
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (operation_id, line_number) DO UPDATE SET
            model_tehniki_id = EXCLUDED.model_tehniki_id,
            model_oborudovaniya_id = EXCLUDED.model_oborudovaniya_id,
            smennaya_norma_vyrabotki = EXCLUDED.smennaya_norma_vyrabotki,
            norma_rashoda_topliva = EXCLUDED.norma_rashoda_topliva,
            prodolzhitelnost_smeny = EXCLUDED.prodolzhitelnost_smeny,
            edinica_smennoy_normy = EXCLUDED.edinica_smennoy_normy,
            baza_rascheta_gsm = EXCLUDED.baza_rascheta_gsm,
            klyuch_svyazi_stroki_norm = EXCLUDED.klyuch_svyazi_stroki_norm,
            _loaded_at = now()
    """

    conn = pg.get_conn()
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()

    logging.info("Loaded %s rows into %s", len(rows), RAW_NORMS_TABLE)


def quality_check(**context):
    fetched = context["ti"].xcom_pull(
        task_ids="extract_work_types",
        key="work_type_count",
    ) or 0
    norms_fetched = context["ti"].xcom_pull(
        task_ids="extract_work_types",
        key="norm_count",
    ) or 0

    if fetched == 0:
        raise ValueError("No work types fetched")

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    record = pg.get_first(
        """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE _deletionmark IS FALSE) AS active_rows
        FROM raw.r1c_tech_operations
        """
    )
    norms_record = pg.get_first(
        "SELECT COUNT(*) FROM raw.r1c_operation_fuel_norms"
    )

    logging.info(
        "Work types quality check: fetched=%s, raw_total=%s, raw_active=%s, norms_fetched=%s, norms_in_db=%s",
        fetched,
        record[0],
        record[1],
        norms_fetched,
        norms_record[0],
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
    description="Полная выгрузка Catalog_ВидыРаботСотрудников + нормы расхода ГСМ по операциям/моделям техники (АпкНормы)",
    start_date=datetime(2026, 8, 19),
    schedule_interval="30 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["1c", "odata", "raw", "catalogs", "work-types", "fuel"],
) as dag:
    extract = PythonOperator(
        task_id="extract_work_types",
        python_callable=fetch_work_types,
    )
    load = PythonOperator(
        task_id="load_work_types",
        python_callable=load_work_types,
    )
    load_norms = PythonOperator(
        task_id="load_fuel_norms",
        python_callable=load_fuel_norms,
    )
    check = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
    )

    extract >> load >> load_norms >> check
