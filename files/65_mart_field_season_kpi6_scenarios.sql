-- KPI6: separate diagnostic agro price-after-writeoff scenario and unified field-season view.
-- Dependencies: mart.v_field_season_kpi6_direct_cost,
-- mart.v_field_fuel_previous_month_scenario, mart.v_field_agro_input_cost_direct.
-- Do not interpret the scenario amounts as booked cost or full cost of production.

CREATE OR REPLACE VIEW mart.v_field_agro_later_price_scenario AS
SELECT
    season_year,
    field_sk,
    pole_id,
    document_id,
    line_number,
    document_date,
    price_document_date,
    nomenklatura_id,
    agro_input_category,
    quantity,
    diagnostic_estimated_cost_rub_no_vat AS scenario_rub_no_vat,
    'PURCHASE_PRICE_AFTER_WRITEOFF_ESTIMATE'::text AS scenario_method
FROM mart.v_field_agro_input_cost_direct
WHERE purchase_price_match_status = 'ONLY_PRICE_AFTER_WRITEOFF'
  AND agro_cost_quality_status = 'PRICE_AFTER_WRITEOFF_NOT_IN_STRICT_COST'
  AND strict_cost_rub_no_vat IS NULL
  AND diagnostic_estimated_cost_rub_no_vat IS NOT NULL;

COMMENT ON VIEW mart.v_field_agro_later_price_scenario IS
'Сценарная оценка агровходов по закупке после списания; не подтверждённая стоимость списания и не часть строгой оценки KPI №6.';

GRANT SELECT ON mart.v_field_agro_later_price_scenario TO datalens_ro;

CREATE OR REPLACE VIEW mart.v_field_season_kpi6_with_scenarios AS
WITH fuel_scenario AS (
    SELECT
        season_year,
        pole_id,
        SUM(allocated_liters) AS scenario_fuel_liters,
        SUM(scenario_fuel_rub_no_vat) AS scenario_fuel_rub_no_vat,
        COUNT(*) AS scenario_fuel_rows
    FROM mart.v_field_fuel_previous_month_scenario
    GROUP BY season_year, pole_id
),
agro_scenario AS (
    SELECT
        season_year,
        field_sk,
        SUM(scenario_rub_no_vat) AS scenario_agro_rub_no_vat,
        COUNT(*) AS scenario_agro_rows
    FROM mart.v_field_agro_later_price_scenario
    WHERE field_sk IS NOT NULL
    GROUP BY season_year, field_sk
)
SELECT
    k.field_sk,
    k.season_year,
    k.pole_id,
    k.field_name,
    k.coverage_status,
    k.stable_economic_area_ha,
    k.labor_rub,
    k.covered_fuel_rub_no_vat,
    k.seeds_rub_no_vat,
    k.fertilizer_rub_no_vat,
    k.crop_protection_rub_no_vat,
    k.allocated_fuel_liters,
    k.agro_rows_without_strict_price,
    k.other_agro_rows,
    k.partial_estimated_direct_cost_rub AS strict_components_partial_rub,
    k.covered_registered_direct_cost_rub,
    k.covered_registered_direct_cost_rub_per_ha,
    f.scenario_fuel_liters,
    f.scenario_fuel_rub_no_vat,
    f.scenario_fuel_rows,
    a.scenario_agro_rub_no_vat,
    a.scenario_agro_rows,
    ROUND(
        k.partial_estimated_direct_cost_rub
        + COALESCE(f.scenario_fuel_rub_no_vat, 0)
        + COALESCE(a.scenario_agro_rub_no_vat, 0),
        2
    ) AS partial_plus_scenarios_rub,
    CASE
        WHEN k.stable_economic_area_ha > 0 THEN
            ROUND(
                (
                    k.partial_estimated_direct_cost_rub
                    + COALESCE(f.scenario_fuel_rub_no_vat, 0)
                    + COALESCE(a.scenario_agro_rub_no_vat, 0)
                ) / k.stable_economic_area_ha,
                2
            )
        ELSE NULL
    END AS partial_plus_scenarios_rub_per_ha,
    'PARTIAL_REGISTERED_COST_PLUS_EXPLICIT_PRICE_SCENARIOS'
        ::text AS combined_method
FROM mart.v_field_season_kpi6_direct_cost AS k
LEFT JOIN fuel_scenario AS f
  ON f.season_year = k.season_year
 AND f.pole_id = k.pole_id
LEFT JOIN agro_scenario AS a
  ON a.season_year = k.season_year
 AND a.field_sk = k.field_sk;

COMMENT ON VIEW mart.v_field_season_kpi6_with_scenarios IS
'Единая витрина поле × сезон: пять компонентов строгой оценки отдельно от сценарных добавок. partial_plus_scenarios — сценарная частичная оценка, НЕ полная фактическая себестоимость. Статус строгого KPI №6 не меняется.';

GRANT SELECT ON mart.v_field_season_kpi6_with_scenarios TO datalens_ro;
