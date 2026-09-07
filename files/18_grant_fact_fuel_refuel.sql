-- Разовый grant на mart.fact_fuel_refuel для BI-роли. Грант также
-- встроен в ночной DAG (files/airflow/dags/dag_extract_zapravochnaya_vedomost.py),
-- этот файл — только для мгновенного присвоения доступа сейчас,
-- без ожидания следующего запуска DAG.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'datalens_ro') THEN
        EXECUTE 'GRANT SELECT ON mart.fact_fuel_refuel TO datalens_ro';
    END IF;
END $$;
