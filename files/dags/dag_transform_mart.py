from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "dag_transform_mart"
POSTGRES_CONN_ID = "ldali_postgres"

SQL_FACT_SPISANIE = """
INSERT INTO mart.fact_spisanie_materialov
    (doc_id,doc_number,doc_date,pole_id,nomenklatura_id,edinica_id,kolichestvo,summa,seriya)
SELECT d._id,d.doc_number,d.doc_date,l.pole_id,l.nomenklatura_id,l.edinica_id,l.kolichestvo,l.summa,l.seriya
FROM raw.r1c_dvizhenie_produkcii d
JOIN raw.r1c_dvizhenie_produkcii_lines l ON l.doc_id = d._id
WHERE d.operaciya = 'СписаниеМатериалов' AND d._posted = TRUE AND d._deletionmark = FALSE
ON CONFLICT (doc_id,pole_id,nomenklatura_id) DO UPDATE SET
    doc_number=EXCLUDED.doc_number,doc_date=EXCLUDED.doc_date,
    edinica_id=EXCLUDED.edinica_id,kolichestvo=EXCLUDED.kolichestvo,
    summa=EXCLUDED.summa,seriya=EXCLUDED.seriya,_updated_at=now();
"""

SQL_FACT_VYPUSK = """
INSERT INTO mart.fact_vypusk_urozhaya
    (doc_id,doc_number,doc_date,pole_id,nomenklatura_id,edinica_id,kolichestvo,summa,seriya)
SELECT d._id,d.doc_number,d.doc_date,l.pole_id,l.nomenklatura_id,l.edinica_id,l.kolichestvo,l.summa,l.seriya
FROM raw.r1c_dvizhenie_produkcii d
JOIN raw.r1c_dvizhenie_produkcii_lines l ON l.doc_id = d._id
WHERE d.operaciya = 'ОприходованиеУрожая' AND d._posted = TRUE AND d._deletionmark = FALSE
ON CONFLICT (doc_id,pole_id,nomenklatura_id) DO UPDATE SET
    doc_number=EXCLUDED.doc_number,doc_date=EXCLUDED.doc_date,
    edinica_id=EXCLUDED.edinica_id,kolichestvo=EXCLUDED.kolichestvo,
    summa=EXCLUDED.summa,seriya=EXCLUDED.seriya,_updated_at=now();
"""

SQL_FACT_PUTEVOY = """
INSERT INTO mart.fact_putevoy_rabota
    (doc_id,doc_number,doc_date,tehnika_id,model_tehniki_id,voditel_id,
     pole_id,agr_operaciya_id,edinica_id,obem_rabot_ga,norma_vyrabotki,
     narabotka_moto_chas,probeg_km,toplivo_vydano,toplivo_vozvrat)
SELECT p._id,p.doc_number,p.doc_date,p.tehnika_id,p.model_tehniki_id,p.voditel_id,
       l.pole_id,l.agr_operaciya_id,l.edinica_id,l.obem_rabot_ga,l.norma_vyrabotki,
       p.narabotka_moto_chas,p.probeg_km,p.toplivo_vydano,p.toplivo_vozvrat
FROM raw.r1c_putevoy_list p
JOIN raw.r1c_putevoy_list_lines l ON l.doc_id = p._id
WHERE p._posted = TRUE AND p._deletionmark = FALSE
ON CONFLICT (doc_id,pole_id,agr_operaciya_id) DO UPDATE SET
    doc_number=EXCLUDED.doc_number,doc_date=EXCLUDED.doc_date,
    tehnika_id=EXCLUDED.tehnika_id,model_tehniki_id=EXCLUDED.model_tehniki_id,
    voditel_id=EXCLUDED.voditel_id,edinica_id=EXCLUDED.edinica_id,
    obem_rabot_ga=EXCLUDED.obem_rabot_ga,norma_vyrabotki=EXCLUDED.norma_vyrabotki,
    narabotka_moto_chas=EXCLUDED.narabotka_moto_chas,probeg_km=EXCLUDED.probeg_km,
    toplivo_vydano=EXCLUDED.toplivo_vydano,toplivo_vozvrat=EXCLUDED.toplivo_vozvrat,_updated_at=now();
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
    [t_sp, t_vy, t_pu]
