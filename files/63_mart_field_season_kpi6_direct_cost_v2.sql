-- KPI #6: field x season; costs are purchase-price estimates, not booked COGS.
-- Run in BEGIN ... ROLLBACK for QA; COMMIT only after QA.
CREATE OR REPLACE VIEW mart.v_field_season_kpi6_direct_cost AS
WITH current_fields AS (
    SELECT field_sk, field_code_1c AS pole_id, field_name
    FROM mart.dim_field
    WHERE is_current = true
), labor AS (
    SELECT df.field_sk, l.season_year,
           COUNT(*) AS labor_rows,
           SUM(l.labor_amount_rub) AS labor_rub,
           COUNT(*) FILTER (WHERE l.labor_amount_rub IS NULL) AS labor_rows_without_amount,
           MIN(l.economic_area_ha) FILTER (WHERE l.economic_area_ha > 0) AS min_economic_area_ha,
           MAX(l.economic_area_ha) FILTER (WHERE l.economic_area_ha > 0) AS max_economic_area_ha,
           COUNT(*) FILTER (WHERE l.economic_area_ha IS NULL OR l.economic_area_ha <= 0) AS labor_rows_without_area
    FROM mart.v_field_day_operation_labor_cost l
    JOIN current_fields df ON df.pole_id = l.pole_id
    GROUP BY df.field_sk, l.season_year
), fuel AS (
    SELECT df.field_sk, f.season_year,
           COUNT(*) AS fuel_rows,
           SUM(f.allocated_brand_field_liters) AS allocated_fuel_liters,
           SUM(f.estimated_covered_fuel_cost_no_vat_rub) AS covered_fuel_rub_no_vat,
           COUNT(*) FILTER (
               WHERE f.allocated_brand_field_liters IS NULL
                  OR f.allocated_brand_field_liters < 0
                  OR (f.allocated_brand_field_liters > 0 AND (
                      f.price_status IS DISTINCT FROM 'EXACT_MONTH_BRAND_PRICE'
                      OR f.purchase_price_no_vat_rub_per_liter IS NULL
                      OR f.estimated_covered_fuel_cost_no_vat_rub IS NULL
                  ))
           ) AS fuel_rows_without_exact_price,
           COUNT(*) FILTER (WHERE f.reconciliation_status IS DISTINCT FROM 'RECONCILED') AS fuel_rows_not_reconciled
    FROM mart.v_field_fuel_cost_allocated_exact_brand f
    JOIN current_fields df ON df.pole_id = f.pole_id
    GROUP BY df.field_sk, f.season_year
), agro AS (
    SELECT a.field_sk, a.season_year,
           COUNT(*) FILTER (WHERE a.agro_input_category = 'SEEDS') AS seeds_rows,
           COUNT(*) FILTER (WHERE a.agro_input_category = 'FERTILIZERS') AS fertilizer_rows,
           COUNT(*) FILTER (WHERE a.agro_input_category = 'CROP_PROTECTION') AS crop_protection_rows,
           COUNT(*) FILTER (WHERE a.strict_cost_rub_no_vat IS NULL) AS agro_rows_without_strict_price,
           SUM(a.strict_cost_rub_no_vat) FILTER (WHERE a.agro_input_category = 'SEEDS') AS seeds_rub_no_vat,
           SUM(a.strict_cost_rub_no_vat) FILTER (WHERE a.agro_input_category = 'FERTILIZERS') AS fertilizer_rub_no_vat,
           SUM(a.strict_cost_rub_no_vat) FILTER (WHERE a.agro_input_category = 'CROP_PROTECTION') AS crop_protection_rub_no_vat
    FROM mart.v_field_agro_input_cost_direct a
    WHERE a.field_sk IS NOT NULL
    GROUP BY a.field_sk, a.season_year
), other_agro AS (
    SELECT u.field_sk, u.season_year, COUNT(*) AS other_agro_rows
    FROM mart.v_field_agro_input_usage_direct u
    WHERE u.field_sk IS NOT NULL
      AND u.agro_input_category NOT IN ('SEEDS', 'FERTILIZERS', 'CROP_PROTECTION')
    GROUP BY u.field_sk, u.season_year
), keys AS (
    SELECT field_sk, season_year FROM labor
    UNION SELECT field_sk, season_year FROM fuel
    UNION SELECT field_sk, season_year FROM agro
    UNION SELECT field_sk, season_year FROM other_agro
), joined AS (
    SELECT k.field_sk, k.season_year, df.pole_id, df.field_name,
           l.labor_rows, l.labor_rub, l.labor_rows_without_amount,
           l.min_economic_area_ha, l.max_economic_area_ha, l.labor_rows_without_area,
           f.fuel_rows, f.allocated_fuel_liters, f.covered_fuel_rub_no_vat,
           f.fuel_rows_without_exact_price, f.fuel_rows_not_reconciled,
           a.seeds_rows, a.fertilizer_rows, a.crop_protection_rows,
           a.agro_rows_without_strict_price, a.seeds_rub_no_vat,
           a.fertilizer_rub_no_vat, a.crop_protection_rub_no_vat,
           COALESCE(o.other_agro_rows, 0) AS other_agro_rows
    FROM keys k
    JOIN current_fields df ON df.field_sk = k.field_sk
    LEFT JOIN labor l ON l.field_sk = k.field_sk AND l.season_year = k.season_year
    LEFT JOIN fuel f ON f.field_sk = k.field_sk AND f.season_year = k.season_year
    LEFT JOIN agro a ON a.field_sk = k.field_sk AND a.season_year = k.season_year
    LEFT JOIN other_agro o ON o.field_sk = k.field_sk AND o.season_year = k.season_year
), assessed AS (
    SELECT j.*,
           CASE WHEN j.labor_rows_without_area = 0
                  AND j.min_economic_area_ha > 0
                  AND j.min_economic_area_ha = j.max_economic_area_ha
                THEN j.min_economic_area_ha ELSE NULL::numeric END AS stable_economic_area_ha,
           ROUND(COALESCE(j.labor_rub, 0)
               + COALESCE(j.covered_fuel_rub_no_vat, 0)
               + COALESCE(j.seeds_rub_no_vat, 0)
               + COALESCE(j.fertilizer_rub_no_vat, 0)
               + COALESCE(j.crop_protection_rub_no_vat, 0), 2)
               AS partial_estimated_direct_cost_rub,
           CASE
               WHEN j.labor_rows IS NULL OR j.fuel_rows IS NULL THEN 'NO_LABOR_OR_FUEL_SOURCE'
               WHEN j.labor_rows_without_amount > 0 OR j.labor_rub IS NULL THEN 'LABOR_AMOUNT_MISSING'
               WHEN j.fuel_rows_without_exact_price > 0 OR j.fuel_rows_not_reconciled > 0
                    OR j.allocated_fuel_liters IS NULL THEN 'FUEL_PRICE_OR_RECONCILIATION_GAP'
               WHEN COALESCE(j.agro_rows_without_strict_price, 0) > 0 OR j.other_agro_rows > 0
                    THEN 'AGRO_PRICE_OR_CLASSIFICATION_GAP'
               WHEN j.labor_rows_without_area > 0 OR j.min_economic_area_ha IS NULL
                    OR j.min_economic_area_ha <> j.max_economic_area_ha
                    THEN 'NO_STABLE_ECONOMIC_AREA'
               ELSE 'COVERED_REGISTERED_DIRECT_COST_ESTIMATE'
           END AS coverage_status
    FROM joined j
)
SELECT a.*,
       CASE WHEN a.stable_economic_area_ha > 0
             THEN ROUND(a.partial_estimated_direct_cost_rub / a.stable_economic_area_ha, 2)
            ELSE NULL::numeric END AS partial_estimated_direct_cost_rub_per_ha,
       CASE WHEN a.coverage_status = 'COVERED_REGISTERED_DIRECT_COST_ESTIMATE'
                 AND a.stable_economic_area_ha > 0
             THEN a.partial_estimated_direct_cost_rub
            ELSE NULL::numeric END AS covered_registered_direct_cost_rub,
       CASE WHEN a.coverage_status = 'COVERED_REGISTERED_DIRECT_COST_ESTIMATE'
                 AND a.stable_economic_area_ha > 0
             THEN ROUND(a.partial_estimated_direct_cost_rub / a.stable_economic_area_ha, 2)
            ELSE NULL::numeric END AS covered_registered_direct_cost_rub_per_ha,
       'ALLOCATED_FUEL_AND_PURCHASE_PRICE_ESTIMATE_NOT_BOOKED_COGS'::text AS method_note
FROM assessed a;

COMMENT ON VIEW mart.v_field_season_kpi6_direct_cost IS
'Field x season: labor + allocated exact-brand fuel purchase-price estimate + strict estimated agro inputs. Partial totals remain visible with coverage flags. A covered registered direct-cost estimate is not full booked COGS; zero recorded agro category is not proof of zero consumption; unidentified unit conversion and unmapped records require separate QA.';
GRANT SELECT ON mart.v_field_season_kpi6_direct_cost TO datalens_ro;
