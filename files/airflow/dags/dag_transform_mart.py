from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_transform_mart"
POSTGRES_CONN_ID = "postgres_dwh"

SQL_FACT_SPISANIE = """
INSERT INTO mart.fact_spisanie_materialov
    (doc_id,doc_number,doc_date,pole_id,nomenklatura_id,line_number,edinica_id,kolichestvo,summa,seriya)
SELECT d._id,d.doc_number,d.doc_date,l.pole_id,l.nomenklatura_id,l.line_number,l.edinica_id,l.kolichestvo,l.summa,l.seriya
FROM raw.r1c_dvizhenie_produkcii d
JOIN raw.r1c_dvizhenie_produkcii_lines l ON l.doc_id = d._id
WHERE d.operaciya = 'ПередачаМатериаловВПроизводство' AND d._posted = TRUE AND d._deletionmark = FALSE
ON CONFLICT (doc_id,line_number,nomenklatura_id) DO UPDATE SET
    doc_number=EXCLUDED.doc_number,doc_date=EXCLUDED.doc_date,pole_id=EXCLUDED.pole_id,
    edinica_id=EXCLUDED.edinica_id,kolichestvo=EXCLUDED.kolichestvo,
    summa=EXCLUDED.summa,seriya=EXCLUDED.seriya,_updated_at=now();
"""

SQL_FACT_VYPUSK = """
INSERT INTO mart.fact_vypusk_urozhaya
    (doc_id,doc_number,doc_date,pole_id,nomenklatura_id,line_number,edinica_id,kolichestvo,summa,seriya)
SELECT d._id,d.doc_number,d.doc_date,l.pole_id,l.nomenklatura_id,l.line_number,l.edinica_id,l.kolichestvo,l.summa,l.seriya
FROM raw.r1c_dvizhenie_produkcii d
JOIN raw.r1c_dvizhenie_produkcii_lines l ON l.doc_id = d._id
WHERE d.operaciya = 'ПередачаПродукцииИзПроизводства' AND d._posted = TRUE AND d._deletionmark = FALSE
ON CONFLICT (doc_id,line_number,nomenklatura_id) DO UPDATE SET
    doc_number=EXCLUDED.doc_number,doc_date=EXCLUDED.doc_date,pole_id=EXCLUDED.pole_id,
    edinica_id=EXCLUDED.edinica_id,kolichestvo=EXCLUDED.kolichestvo,
    summa=EXCLUDED.summa,seriya=EXCLUDED.seriya,_updated_at=now();
"""

SQL_FACT_PUTEVOY = """
INSERT INTO mart.fact_putevoy_rabota
    (doc_id,doc_number,doc_date,tehnika_id,model_tehniki_id,voditel_id,
     pole_id,agr_operaciya_id,line_number,edinica_id,obem_rabot_ga,norma_vyrabotki,
     narabotka_moto_chas,probeg_km,toplivo_vydano,toplivo_vozvrat)
SELECT p._id,p.doc_number,p.doc_date,p.tehnika_id,p.model_tehniki_id,p.voditel_id,
       l.pole_id,l.agr_operaciya_id,l.line_number,l.edinica_id,l.obem_rabot_ga,l.norma_vyrabotki,
       p.narabotka_moto_chas,p.probeg_km,p.toplivo_vydano,p.toplivo_vozvrat
FROM raw.r1c_putevoy_list p
JOIN raw.r1c_putevoy_list_lines l ON l.doc_id = p._id
WHERE p._posted = TRUE AND p._deletionmark = FALSE
ON CONFLICT (doc_id,line_number,agr_operaciya_id) DO UPDATE SET
    doc_number=EXCLUDED.doc_number,doc_date=EXCLUDED.doc_date,pole_id=EXCLUDED.pole_id,
    tehnika_id=EXCLUDED.tehnika_id,model_tehniki_id=EXCLUDED.model_tehniki_id,
    voditel_id=EXCLUDED.voditel_id,edinica_id=EXCLUDED.edinica_id,
    obem_rabot_ga=EXCLUDED.obem_rabot_ga,norma_vyrabotki=EXCLUDED.norma_vyrabotki,
    narabotka_moto_chas=EXCLUDED.narabotka_moto_chas,probeg_km=EXCLUDED.probeg_km,
    toplivo_vydano=EXCLUDED.toplivo_vydano,toplivo_vozvrat=EXCLUDED.toplivo_vozvrat,_updated_at=now();
"""



SQL_FACT_FUEL_WRITEOFF = """
INSERT INTO mart.fact_fuel_writeoff (
    period_month,
    equipment_sk,
    fuel_brand_id,
    liters_start,
    liters_end,
    liters_refueled,
    liters_consumed,
    src_doc_ref
)
SELECT
    s.period_month,
    e.eq_sk,
    s.marka_topliva_id,
    s.nachalny_ostatok,
    s.konechny_ostatok,
    s.zapravleno,
    s.fakticheskiy_raskhod,
    s.doc_id::text || '-' || s.line_number::text
FROM staging.v_fuel_writeoff_clean s
LEFT JOIN mart.dim_equipment e
    ON e.code_1c::text = s.tehnika_id::text
WHERE NOT EXISTS (
    SELECT 1
    FROM mart.fact_fuel_writeoff f
    WHERE f.src_doc_ref =
          s.doc_id::text || '-' || s.line_number::text
);
"""

def _run_sql(sql, label):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn(); cur = conn.cursor()
    cur.execute(sql)
    logging.info("%s: affected rows = %s", label, cur.rowcount)
    conn.commit(); cur.close(); conn.close()

def _transform_spisanie(**context): _run_sql(SQL_FACT_SPISANIE, "fact_spisanie_materialov")
def _transform_vypusk(**context): _run_sql(SQL_FACT_VYPUSK, "fact_vypusk_urozhaya")
def _transform_putevoy(**context): _run_sql(SQL_FACT_PUTEVOY, "fact_putevoy_rabota")

def _transform_fuel_writeoff(**context):
    _run_sql(SQL_FACT_FUEL_WRITEOFF, "fact_fuel_writeoff")



default_args = {"owner": "bi", "depends_on_past": False, "retries": 2, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id=DAG_ID, default_args=default_args,
    description="Трансформация RAW → MART: split ДвиженияПродукции по Операции + путевые листы",
    start_date=datetime(2026, 7, 1), schedule_interval="30 3 * * *",
    catchup=False, max_active_runs=1, tags=["1c", "transform", "mart"],
) as dag:
    t_sp = PythonOperator(task_id="transform_spisanie_materialov", python_callable=_transform_spisanie, provide_context=True)
    t_vy = PythonOperator(task_id="transform_vypusk_urozhaya", python_callable=_transform_vypusk, provide_context=True)
    t_pu = PythonOperator(task_id="transform_putevoy_rabota", python_callable=_transform_putevoy, provide_context=True)
    t_fw = PythonOperator(task_id="transform_fuel_writeoff", python_callable=_transform_fuel_writeoff, provide_context=True)
    [t_sp, t_vy, t_pu, t_fw]
