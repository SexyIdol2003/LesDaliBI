BEGIN;

CREATE VIEW mart.v_kpi5_fuel_field_allocation_scenario AS
WITH field_rows AS (
    SELECT
        f.month_start,
        f.eq_sk,
        f.fuel_brand_id,
        f.pole_id,
        f.agr_operaciya_id,
        f.allocated_brand_field_liters,
        SUM(f.allocated_brand_field_liters) OVER (
            PARTITION BY f.month_start, f.eq_sk, f.fuel_brand_id
        ) AS all_field_liters
    FROM mart.v_field_fuel_cost_allocated_exact_brand f
    WHERE f.allocated_brand_field_liters > 0
),
joined AS (
    SELECT
        s.period_month,
        s.season_year,
        s.equipment_sk,
        s.equipment_name,
        s.fuel_brand_id,
        s.writeoff_liters,
        s.price_method,
        s.price_month,
        s.price_age_months,
        s.price_no_vat_rub_per_liter,
        f.pole_id,
        f.agr_operaciya_id,
        CASE
            WHEN f.all_field_liters > 0 THEN
                f.allocated_brand_field_liters
                * LEAST(
                    1::numeric,
                    s.writeoff_liters / NULLIF(f.all_field_liters, 0)
                )
        END AS field_liters
    FROM mart.v_kpi5_fuel_purchase_price_scenario s
    LEFT JOIN field_rows f
      ON f.month_start = s.period_month
     AND f.eq_sk = s.equipment_sk
     AND f.fuel_brand_id = s.fuel_brand_id
),
output_rows AS (
    SELECT
        period_month, season_year, equipment_sk, equipment_name,
        fuel_brand_id, pole_id, agr_operaciya_id,
        'FIELD_ALLOCATED'::text AS allocation_status,
        price_method, price_month, price_age_months,
        price_no_vat_rub_per_liter,
        field_liters AS liters
    FROM joined
    WHERE field_liters > 0

    UNION ALL

    SELECT
        period_month, season_year, equipment_sk, equipment_name,
        fuel_brand_id, NULL AS pole_id, NULL AS agr_operaciya_id,
        'NOT_ALLOCATED_TO_FIELD'::text AS allocation_status,
        price_method, price_month, price_age_months,
        price_no_vat_rub_per_liter,
        GREATEST(
            MAX(writeoff_liters) - COALESCE(SUM(field_liters), 0),
            0
        ) AS liters
    FROM joined
    GROUP BY
        period_month, season_year, equipment_sk, equipment_name,
        fuel_brand_id, price_method, price_month,
        price_age_months, price_no_vat_rub_per_liter
    HAVING GREATEST(
        MAX(writeoff_liters) - COALESCE(SUM(field_liters), 0),
        0
    ) > 0
)
SELECT
    period_month,
    season_year,
    equipment_sk,
    equipment_name,
    fuel_brand_id,
    pole_id,
    agr_operaciya_id,
    allocation_status,
    price_method,
    price_month,
    price_age_months,
    price_no_vat_rub_per_liter,
    liters,
    liters * price_no_vat_rub_per_liter
        AS estimated_fuel_cost_no_vat_rub
FROM output_rows;

COMMENT ON VIEW mart.v_kpi5_fuel_field_allocation_scenario IS
    'Оценка ГСМ по технике, месяцу, марке и подтверждённо распределённому полю; неприписанные полям литры сохранены отдельно. Цена закупочная без НДС, сценарий прошлой закупки помечен. Не фактическая учётная себестоимость и не замена KPI5/KPI6.';

GRANT SELECT
ON mart.v_kpi5_fuel_field_allocation_scenario
TO datalens_ro;

COMMIT;
