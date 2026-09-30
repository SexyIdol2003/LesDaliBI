BEGIN;

CREATE VIEW mart.v_kpi5_fuel_purchase_price_scenario AS
WITH writeoff AS (
    SELECT
        f.period_month,
        f.equipment_sk,
        f.fuel_brand_id::text AS fuel_brand_id,
        SUM(COALESCE(f.liters_consumed, 0)) AS writeoff_liters
    FROM mart.fact_fuel_writeoff f
    WHERE f.period_month IS NOT NULL
      AND f.fuel_brand_id IS NOT NULL
    GROUP BY f.period_month, f.equipment_sk, f.fuel_brand_id
),
strict_price AS (
    SELECT
        c.period_month,
        c.fuel_brand_id,
        c.has_exact_purchase_price,
        c.purchase_price_without_vat_rub_per_liter
    FROM mart.v_fuel_writeoff_purchase_price_coverage c
),
priced AS (
    SELECT
        w.period_month,
        w.equipment_sk,
        w.fuel_brand_id,
        e.name AS equipment_name,
        w.writeoff_liters,
        s.has_exact_purchase_price,
        s.purchase_price_without_vat_rub_per_liter AS exact_price_no_vat,
        prior.purchase_month AS prior_purchase_month,
        prior.price_no_vat AS prior_price_no_vat
    FROM writeoff w
    LEFT JOIN mart.dim_equipment e
      ON e.eq_sk = w.equipment_sk
    LEFT JOIN strict_price s
      ON s.period_month = w.period_month
     AND s.fuel_brand_id = w.fuel_brand_id
    LEFT JOIN LATERAL (
        SELECT
            p.purchase_month,
            SUM(p.amount_without_vat_rub)
                / NULLIF(SUM(p.quantity), 0) AS price_no_vat
        FROM mart.v_fuel_purchase_by_month p
        WHERE p.nomenklatura_id = w.fuel_brand_id
          AND p.fuel_kind = 'diesel'
          AND p.purchase_month < w.period_month
          AND p.quantity > 0
          AND p.amount_without_vat_rub > 0
        GROUP BY p.purchase_month
        ORDER BY p.purchase_month DESC
        LIMIT 1
    ) prior ON s.has_exact_purchase_price IS NOT TRUE
)
SELECT
    period_month,
    EXTRACT(YEAR FROM period_month)::int AS season_year,
    equipment_sk,
    equipment_name,
    fuel_brand_id,
    ROUND(writeoff_liters, 4) AS writeoff_liters,
    CASE
        WHEN has_exact_purchase_price IS TRUE
            THEN 'EXACT_MONTH_BRAND_PURCHASE'
        WHEN prior_price_no_vat IS NOT NULL
            THEN 'PRIOR_SAME_BRAND_PURCHASE_SCENARIO'
        ELSE 'NO_PURCHASE_PRICE'
    END AS price_method,
    CASE
        WHEN has_exact_purchase_price IS TRUE THEN period_month
        ELSE prior_purchase_month
    END AS price_month,
    CASE
        WHEN has_exact_purchase_price IS TRUE THEN 0
        WHEN prior_purchase_month IS NOT NULL THEN
            EXTRACT(YEAR FROM age(period_month, prior_purchase_month))::int * 12
            + EXTRACT(MONTH FROM age(period_month, prior_purchase_month))::int
    END AS price_age_months,
    CASE
        WHEN has_exact_purchase_price IS TRUE THEN exact_price_no_vat
        ELSE prior_price_no_vat
    END AS price_no_vat_rub_per_liter,
    CASE
        WHEN has_exact_purchase_price IS TRUE
            THEN ROUND(writeoff_liters * exact_price_no_vat, 2)
        WHEN prior_price_no_vat IS NOT NULL
            THEN ROUND(writeoff_liters * prior_price_no_vat, 2)
        ELSE NULL
    END AS estimated_fuel_cost_no_vat_rub
FROM priced;

COMMENT ON VIEW mart.v_kpi5_fuel_purchase_price_scenario IS
    'Оценка ГСМ по технике: точная закупка месяца/марки либо явно помеченный сценарий последней предшествующей закупки того же UUID. Не учётная стоимость списания; не подменяет KPI5/KPI6.';

GRANT SELECT ON mart.v_kpi5_fuel_purchase_price_scenario TO datalens_ro;

DO $check$
DECLARE
    source_liters numeric;
    result_liters numeric;
    duplicate_keys bigint;
    missing_price_rows bigint;
BEGIN
    SELECT SUM(liters_consumed)
    INTO source_liters
    FROM mart.fact_fuel_writeoff
    WHERE period_month IS NOT NULL
      AND fuel_brand_id IS NOT NULL;

    SELECT SUM(writeoff_liters)
    INTO result_liters
    FROM mart.v_kpi5_fuel_purchase_price_scenario;

    SELECT COUNT(*)
    INTO duplicate_keys
    FROM (
        SELECT period_month, equipment_sk, fuel_brand_id
        FROM mart.v_kpi5_fuel_purchase_price_scenario
        GROUP BY period_month, equipment_sk, fuel_brand_id
        HAVING COUNT(*) > 1
    ) d;

    SELECT COUNT(*)
    INTO missing_price_rows
    FROM mart.v_kpi5_fuel_purchase_price_scenario
    WHERE writeoff_liters > 0
      AND price_method = 'NO_PURCHASE_PRICE';

    IF source_liters IS DISTINCT FROM result_liters
       OR duplicate_keys <> 0
       OR missing_price_rows <> 0 THEN
        RAISE EXCEPTION
            'QA failed: source_liters=%, result_liters=%, duplicate_keys=%, missing_price_rows=%',
            source_liters, result_liters, duplicate_keys, missing_price_rows;
    END IF;

    RAISE NOTICE
        'QA passed: source_liters=%, result_liters=%, duplicate_keys=%, missing_price_rows=%',
        source_liters, result_liters, duplicate_keys, missing_price_rows;
END
$check$;

COMMIT;

SELECT
    season_year,
    price_method,
    ROUND(SUM(writeoff_liters)::numeric, 2) AS liters,
    ROUND(SUM(estimated_fuel_cost_no_vat_rub)::numeric, 2)
        AS estimated_cost_no_vat_rub,
    MAX(price_age_months) AS max_price_age_months
FROM mart.v_kpi5_fuel_purchase_price_scenario
GROUP BY season_year, price_method
ORDER BY season_year, price_method;
