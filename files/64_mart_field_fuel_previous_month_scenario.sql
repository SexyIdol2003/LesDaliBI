-- Отдельная сценарная оценка, НЕ фактическая себестоимость ГСМ.
-- Только отсутствие точной цены текущего месяца; та же марка,
-- строго предыдущий календарный месяц с подтверждённой закупочной ценой.
-- Гранулярность: поле × месяц × марка топлива.
CREATE OR REPLACE VIEW mart.v_field_fuel_previous_month_scenario AS
WITH missing AS (
    SELECT
        f.season_year,
        f.month_start,
        f.pole_id,
        f.fuel_brand_id,
        SUM(f.allocated_brand_field_liters) AS allocated_liters,
        COUNT(*) AS source_rows
    FROM mart.v_field_fuel_cost_allocated_exact_brand AS f
    WHERE f.price_status = 'NO_EXACT_MONTH_BRAND_PRICE'
      AND f.allocated_brand_field_liters > 0
    GROUP BY f.season_year, f.month_start, f.pole_id, f.fuel_brand_id
),
priced AS (
    SELECT
        m.*,
        p.period_month AS price_month,
        p.last_purchase_date,
        p.purchase_price_without_vat_rub_per_liter AS price_rub_per_liter
    FROM missing AS m
    JOIN mart.v_fuel_writeoff_purchase_price_coverage AS p
      ON p.period_month = (m.month_start - INTERVAL '1 month')::date
     AND p.fuel_brand_id = m.fuel_brand_id
     AND p.has_exact_purchase_price = true
)
SELECT
    season_year,
    month_start,
    pole_id,
    fuel_brand_id,
    allocated_liters,
    source_rows,
    price_month,
    last_purchase_date,
    price_rub_per_liter,
    ROUND(allocated_liters * price_rub_per_liter, 2)
        AS scenario_fuel_rub_no_vat,
    'PREVIOUS_MONTH_PURCHASE_PRICE_ESTIMATE'::text AS scenario_method
FROM priced;

COMMENT ON VIEW mart.v_field_fuel_previous_month_scenario IS
'Отдельный сценарий: списанные литры, распределённые на поле, умножены на цену закупки той же марки за предыдущий календарный месяц. Не фактическая стоимость и не точная цена месяца списания. Не включать автоматически в KPI №6.';

GRANT SELECT ON mart.v_field_fuel_previous_month_scenario TO datalens_ro;
