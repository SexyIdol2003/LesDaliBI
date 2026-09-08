-- ============================================================================
-- 21_harvest_area_yield.sql
-- Финальная витрина урожайности (т/га): связывает факт урожая с площадью
-- поля на дату взвешивания, без задвоения площади.
--
-- ВАЖНО: в табличной части ИсторияПоля поля Начало/КонецПериодаАктуальности
-- иногда идут в обратном порядке — сравнение через LEAST/GREATEST.
-- DROP VIEW ... CASCADE обязателен, потому что CREATE OR REPLACE VIEW не даёт
-- переименовать существующие колонки view (nachalo_perioda -> period_start).
-- См. SESSION_2026-09-08_POLYA_AREA_DISCOVERY.md.
-- ============================================================================

ALTER TABLE raw.r1c_polya_istoriya ADD COLUMN IF NOT EXISTS eto_roditel boolean;
ALTER TABLE raw.r1c_polya_istoriya ADD COLUMN IF NOT EXISTS eto_vetv boolean;

DROP VIEW IF EXISTS mart.v_fact_harvest_yield CASCADE;
DROP VIEW IF EXISTS mart.v_field_area_by_date CASCADE;

-- ---- Витрина: площадь поля на конкретную дату, без задвоения ----
CREATE VIEW mart.v_field_area_by_date AS
SELECT
    h.pole_id,
    p.description AS field_name,
    h.god_urozhaya,
    h.kultura_id,
    h.ploshad_obshaya AS area_ha,
    LEAST(h.nachalo_perioda, h.konec_perioda) AS period_start,
    GREATEST(h.nachalo_perioda, h.konec_perioda) AS period_end
FROM raw.r1c_polya_istoriya h
JOIN raw.r1c_polya p ON p._id = h.pole_id
WHERE COALESCE(h.ne_ispolzuetsya, false) = false
  AND COALESCE(h.eto_roditel, true) = true
  AND COALESCE(h.eto_vetv, false) = false;

GRANT SELECT ON mart.v_field_area_by_date TO datalens_ro;

-- ---- Финальная витрина: факт урожая + площадь на дату взвешивания + т/га ----
CREATE VIEW mart.v_fact_harvest_yield AS
WITH harvest_by_field_year AS (
    SELECT
        h.pole_id,
        h.god_urozhaya,
        MIN(h.doc_date) AS first_doc_date,
        SUM(h.kolichestvo_kg) AS total_kg,
        COUNT(*) AS weighings_cnt
    FROM mart.v_fact_harvest_tok h
    WHERE h.is_pole_resolved
    GROUP BY h.pole_id, h.god_urozhaya
)
SELECT
    hy.pole_id,
    a.field_name,
    hy.god_urozhaya,
    hy.total_kg,
    hy.weighings_cnt,
    a.area_ha,
    a.kultura_id,
    CASE WHEN a.area_ha IS NULL OR a.area_ha = 0 THEN NULL
         ELSE ROUND((hy.total_kg / 1000.0) / a.area_ha, 3)
    END AS yield_t_ha
FROM harvest_by_field_year hy
LEFT JOIN mart.v_field_area_by_date a
    ON a.pole_id = hy.pole_id
   AND hy.first_doc_date::date BETWEEN a.period_start::date AND a.period_end::date;

GRANT SELECT ON mart.v_fact_harvest_yield TO datalens_ro;
