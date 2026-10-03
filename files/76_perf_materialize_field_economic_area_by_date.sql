-- Ускорение KPI6: площадь поля по датам материализована,
-- прежнее имя v_field_economic_area_by_date сохранено как тонкое представление.
-- Не запускать повторно файл 52: он делает DROP VIEW.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout = '600s';

CREATE TEMP TABLE _area_before AS
SELECT count(*) AS n,
       round(sum(economic_area_ha), 4) AS s,
       count(DISTINCT pole_id) AS fields
FROM mart.v_field_economic_area_by_date;

SELECT 'CREATE MATERIALIZED VIEW mart.mv_field_economic_area_by_date AS '
       || pg_get_viewdef('mart.v_field_economic_area_by_date'::regclass) AS ddl
\gexec

CREATE UNIQUE INDEX ux_mv_field_economic_area_by_date
    ON mart.mv_field_economic_area_by_date (pole_id, snapshot_date);
CREATE INDEX ix_mv_field_economic_area_by_date_date
    ON mart.mv_field_economic_area_by_date (snapshot_date);

CREATE OR REPLACE VIEW mart.v_field_economic_area_by_date AS
SELECT * FROM mart.mv_field_economic_area_by_date;

DO $$
DECLARE b record; a record;
BEGIN
    SELECT * INTO b FROM _area_before;
    SELECT count(*) AS n, round(sum(economic_area_ha), 4) AS s,
           count(DISTINCT pole_id) AS fields
      INTO a FROM mart.v_field_economic_area_by_date;
    IF a.n <> b.n OR a.s IS DISTINCT FROM b.s OR a.fields <> b.fields THEN
        RAISE EXCEPTION 'Mismatch: before % / % / %, after % / % / %',
            b.n, b.s, b.fields, a.n, a.s, a.fields;
    END IF;
END $$;

GRANT SELECT ON mart.v_field_economic_area_by_date TO datalens_ro;
COMMIT;
ANALYZE mart.mv_field_economic_area_by_date;
