-- ============================================================================
-- 21_harvest_area_yield.sql
-- Финальная витрина урожайности (т/га): связывает факт урожая с площадью
-- поля на дату взвешивания.
--
-- ВАЖНО (исправлено 2026-09-08, третий раунд диагностики):
-- Одно поле в один и тот же период часто разбито на несколько контуров с разными
-- культурами одновременно. Джойн по дате даёт несколько строк на поле-год — нужно
-- СУММИРОВАТЬ площадь всех подходящих контуров, а не делить весь урожай поля на
-- площадь одного случайного контура (иначе получались абсурдные 1000+ т/га).
-- См. SESSION_2026-09-08_POLYA_AREA_DISCOVERY.md.
-- ============================================================================

ALTER TABLE raw.r1c_polya_istoriya ADD COLUMN IF NOT EXISTS eto_roditel boolean;
ALTER TABLE raw.r1c_polya_istoriya ADD COLUMN IF NOT EXISTS eto_vetv boolean;

DROP VIEW IF EXISTS mart.v_fact_harvest_yield CASCADE;
DROP VIEW IF EXISTS mart.v_field_area_by_date CASCADE;

-- ---- Витрина: площадь поля на конкретную дату (одна строка = один контур) ----
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
WHERE COALESCE(h.eto_roditel, false) = false
  AND h.ploshad_obshaya > 0
  AND h.nachalo_perioda > '0002-01-01'::timestamp
  AND h.konec_perioda   > '0002-01-01'::timestamp;

GRANT SELECT ON mart.v_field_area_by_date TO datalens_ro;

-- ---- Финальная витрина: факт урожая + суммарная площадь на дату взвешивания + т/га ----
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
),
area_matched AS (
    SELECT
        hy.pole_id,
        hy.god_urozhaya,
        MAX(a.field_name) AS field_name,
        SUM(a.area_ha) AS area_ha,
        COUNT(DISTINCT a.kultura_id) AS distinct_crops
    FROM harvest_by_field_year hy
    JOIN mart.v_field_area_by_date a
        ON a.pole_id = hy.pole_id
       AND hy.first_doc_date::date BETWEEN a.period_start::date AND a.period_end::date
    GROUP BY hy.pole_id, hy.god_urozhaya
)
SELECT
    hy.pole_id,
    am.field_name,
    hy.god_urozhaya,
    hy.total_kg,
    hy.weighings_cnt,
    am.area_ha,
    am.distinct_crops,
    CASE WHEN am.area_ha IS NULL OR am.area_ha = 0 THEN NULL
         ELSE ROUND((hy.total_kg / 1000.0) / am.area_ha, 3)
    END AS yield_t_ha
FROM harvest_by_field_year hy
LEFT JOIN area_matched am
    ON am.pole_id = hy.pole_id AND am.god_urozhaya = hy.god_urozhaya;

GRANT SELECT ON mart.v_fact_harvest_yield TO datalens_ro;
