-- 51_fix_equipment_costs_summary_nulls.sql
-- Исправление итоговой стоимости техники.
--
-- Проблема:
-- total_equipment_cost_rub рассчитывался как:
--   SUM(labor_cost_this_row) + MAX(fuel_cost_rub_month_total)
--
-- Когда стоимость ГСМ отсутствует, MAX(...) = NULL.
-- В PostgreSQL любое число + NULL = NULL, поэтому зарплата
-- ошибочно пропадала из итоговой стоимости техники.
--
-- Решение:
-- Все компоненты приводятся к нулю через COALESCE.
-- Ремонты пока оставлены нулевыми до появления заполненного источника.

CREATE OR REPLACE VIEW mart.v_fact_equipment_costs_by_period_summary AS
SELECT
    period_month,
    code_1c,
    eq_sk,
    equipment_name,

    COALESCE(SUM(labor_cost_this_row), 0) AS total_labor_cost_rub,

    0::numeric AS total_repair_cost_rub,

    COALESCE(
        MAX(fuel_cost_rub_month_total),
        0
    ) AS total_fuel_cost_rub,

    COALESCE(SUM(labor_cost_this_row), 0)
    + 0::numeric
    + COALESCE(MAX(fuel_cost_rub_month_total), 0)
        AS total_equipment_cost_rub,

    COALESCE(SUM(gektarov), 0) AS gektarov,

    CASE
        WHEN COALESCE(SUM(gektarov), 0) > 0
            THEN ROUND(
                (
                    COALESCE(SUM(labor_cost_this_row), 0)
                    + COALESCE(MAX(fuel_cost_rub_month_total), 0)
                )
                / SUM(gektarov),
                2
            )
        ELSE NULL
    END AS total_equipment_rub_per_ha,

    CASE
        WHEN COALESCE(MAX(fuel_cost_rub_month_total), 0) > 0
            THEN 'LABOR_PLUS_PARTIAL_FUEL_COST'
        ELSE 'LABOR_ONLY_FUEL_PRICE_NOT_AVAILABLE'
    END AS cost_coverage_status

FROM mart.v_fact_equipment_costs_by_period
GROUP BY
    period_month,
    code_1c,
    eq_sk,
    equipment_name;

COMMENT ON VIEW mart.v_fact_equipment_costs_by_period_summary IS
    'Учтённые затраты на технику по месяцам: зарплата + доступная стоимость ГСМ. Ремонты пока не загружены. При отсутствии цены ГСМ итог содержит зарплату и статус покрытия.';

GRANT SELECT ON mart.v_fact_equipment_costs_by_period_summary
TO datalens_ro;
