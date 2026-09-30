-- Выполнять отдельно от миграции №75. При долгом расчёте запрос прервётся.
SET statement_timeout = '120s';

WITH rows AS MATERIALIZED (
    SELECT
        period_month,
        equipment_sk,
        fuel_brand_id,
        writeoff_liters AS liters,
        'source'::text AS origin,
        NULL::text AS allocation_status,
        NULL::boolean AS pole_missing
    FROM mart.v_kpi5_fuel_purchase_price_scenario

    UNION ALL

    SELECT
        period_month,
        equipment_sk,
        fuel_brand_id,
        liters,
        'output'::text AS origin,
        allocation_status,
        pole_id IS NULL AS pole_missing
    FROM mart.v_kpi5_fuel_field_allocation_scenario
),
key_totals AS (
    SELECT
        period_month,
        equipment_sk,
        fuel_brand_id,
        SUM(liters) FILTER (WHERE origin = 'source') AS source_liters,
        SUM(liters) FILTER (WHERE origin = 'output') AS output_liters,
        COUNT(*) FILTER (
            WHERE origin = 'output'
              AND allocation_status = 'FIELD_ALLOCATED'
              AND pole_missing
        ) AS field_without_pole,
        COUNT(*) FILTER (
            WHERE origin = 'output'
              AND allocation_status = 'NOT_ALLOCATED_TO_FIELD'
              AND NOT pole_missing
        ) AS remainder_with_pole
    FROM rows
    GROUP BY period_month, equipment_sk, fuel_brand_id
)
SELECT
    COUNT(*) FILTER (
        WHERE source_liters IS NULL
           OR ABS(source_liters - COALESCE(output_liters, 0)) > 0.000001
    ) AS bad_keys,
    COALESCE(SUM(field_without_pole), 0) AS field_without_pole,
    COALESCE(SUM(remainder_with_pole), 0) AS remainder_with_pole,
    SUM(source_liters) AS source_liters,
    SUM(output_liters) AS output_liters
FROM key_totals;
