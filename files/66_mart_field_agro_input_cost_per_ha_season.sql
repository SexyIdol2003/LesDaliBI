-- ============================================================================
-- 66_mart_field_agro_input_cost_per_ha_season.sql
--
-- Агрозатраты (семена, удобрения, СЗР) на 1 экономический гектар, поле x сезон.
--
-- Площадь привязывается к конкретной дате списания каждой строки агровхода
-- (mart.v_field_economic_area_by_date по pole_id + document_date), а не к
-- произвольным дням календарного года. Стабильность площади (min = max)
-- проверяется только среди дат, где физически были списания за сезон.
--
-- Если площадь нестабильна в рамках сезона — stable_economic_area_ha и
-- agro_cost_rub_per_ha равны NULL, поле не заменяется нулём или средним.
-- ============================================================================

DROP VIEW IF EXISTS mart.v_field_agro_input_cost_per_ha_season;

CREATE VIEW mart.v_field_agro_input_cost_per_ha_season AS
WITH agro_priced AS (
    SELECT
        u.field_sk,
        u.season_year,
        u.pole_id,
        u.document_date,
        u.agro_input_category,
        u.strict_cost_rub_no_vat
    FROM mart.v_field_agro_input_cost_direct u
    WHERE u.field_sk IS NOT NULL
),
agro_with_area AS (
    SELECT
        a.*,
        d.economic_area_ha
    FROM agro_priced a
    LEFT JOIN mart.v_field_economic_area_by_date d
      ON d.pole_id = a.pole_id
     AND d.snapshot_date = a.document_date
),
agro AS (
    SELECT
        field_sk,
        season_year,
        SUM(strict_cost_rub_no_vat) FILTER (WHERE agro_input_category = 'SEEDS') AS seeds_rub,
        SUM(strict_cost_rub_no_vat) FILTER (WHERE agro_input_category = 'FERTILIZERS') AS fertilizers_rub,
        SUM(strict_cost_rub_no_vat) FILTER (WHERE agro_input_category = 'CROP_PROTECTION') AS crop_protection_rub,
        SUM(strict_cost_rub_no_vat) AS total_strict_cost_rub,
        COUNT(*) FILTER (WHERE strict_cost_rub_no_vat IS NULL) AS rows_without_strict_price,
        MIN(economic_area_ha) FILTER (WHERE economic_area_ha > 0) AS min_area_ha,
        MAX(economic_area_ha) FILTER (WHERE economic_area_ha > 0) AS max_area_ha,
        COUNT(*) FILTER (WHERE economic_area_ha IS NULL OR economic_area_ha <= 0) AS rows_without_area
    FROM agro_with_area
    GROUP BY field_sk, season_year
)
SELECT
    a.field_sk,
    a.season_year,
    df.field_code_1c AS pole_id,
    df.field_name,
    a.seeds_rub,
    a.fertilizers_rub,
    a.crop_protection_rub,
    a.total_strict_cost_rub,
    a.rows_without_strict_price,
    a.rows_without_area,
    CASE WHEN a.min_area_ha > 0 AND a.min_area_ha = a.max_area_ha
         THEN a.min_area_ha ELSE NULL END AS stable_economic_area_ha,
    ROUND(
        a.total_strict_cost_rub / NULLIF(
            CASE WHEN a.min_area_ha = a.max_area_ha THEN a.min_area_ha ELSE NULL END, 0
        ), 2
    ) AS agro_cost_rub_per_ha
FROM agro a
JOIN mart.dim_field df ON df.field_sk = a.field_sk AND df.is_current = true;

COMMENT ON VIEW mart.v_field_agro_input_cost_per_ha_season IS
    'Агрозатраты (семена/удобрения/СЗР) на 1 экономический гектар, поле x сезон. Площадь берётся на дату конкретного списания, стабильность проверяется только среди дат со списаниями за сезон. При нестабильной площади значение NULL, не 0 и не среднее.';

GRANT SELECT ON mart.v_field_agro_input_cost_per_ha_season TO datalens_ro;
