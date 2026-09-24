-- 50_field_month_operation_fuel_plan_fact_allocated.sql
-- Расчётный plan-fact ГСМ по полю, месяцу, технике и операции.
--
-- Метод:
-- 1) Берутся только ключи месяц × техника × операция, где площадь и норматив
--    полностью сверены между confirmed plan-fact и полевыми строками ПУЛ.
-- 2) Месячный факт ГСМ техники, уже распределённый по операции,
--    распределяется между полями пропорционально площади работ.
--
-- ВАЖНО:
-- allocated_fact_liters — не прямое списание ГСМ на поле.
-- Это расчётная аллокация месячного факта техники по площади.
-- Ключи с расхождением площади/норматива или без поля не входят в основную витрину
-- и доступны в mart.v_field_month_operation_fuel_plan_fact_quality.

CREATE OR REPLACE VIEW mart.v_field_month_operation_fuel_plan_fact_quality AS
WITH field_work AS (
    SELECT
        date_trunc('month', l.work_date)::date AS month_start,
        e.eq_sk,
        l.agr_operaciya_id,
        COUNT(*) AS field_lines,
        COUNT(DISTINCT l.pole_id) AS field_count,
        SUM(COALESCE(l.gektarov, 0)) AS field_hectares,
        SUM(COALESCE(l.normativny_raskhod_gsm, 0)) AS field_norm_liters
    FROM staging.stg_waybill_field_link l
    JOIN raw.r1c_putevoy_list h
      ON h._id = l.put_list_doc_id
    JOIN mart.dim_equipment e
      ON e.code_1c = h.tehnika_id::text
    WHERE l.link_status = 'resolved_one_field_active'
      AND l.work_date IS NOT NULL
    GROUP BY
        date_trunc('month', l.work_date)::date,
        e.eq_sk,
        l.agr_operaciya_id
)
SELECT
    c.month_start,
    EXTRACT(YEAR FROM c.month_start)::integer AS season_year,
    c.eq_sk,
    c.equipment,
    c.agr_operaciya_id,
    c.operation_name,
    c.hectares_op AS confirmed_hectares,
    c.fact_liters AS confirmed_allocated_fact_liters,
    c.norm_liters AS confirmed_norm_liters,
    c.variance_liters AS confirmed_variance_liters,
    c.variance_pct AS confirmed_variance_pct,
    c.fuel_liters_per_ha AS confirmed_fact_liters_per_ha,
    c.reliability_flag,
    c.fact_method,
    COALESCE(f.field_lines, 0) AS linked_field_lines,
    COALESCE(f.field_count, 0) AS linked_field_count,
    COALESCE(f.field_hectares, 0) AS linked_field_hectares,
    COALESCE(f.field_norm_liters, 0) AS linked_field_norm_liters,
    ROUND(COALESCE(f.field_hectares, 0) - c.hectares_op, 4) AS hectares_delta,
    ROUND(COALESCE(f.field_norm_liters, 0) - c.norm_liters, 4) AS norm_liters_delta,
    CASE
        WHEN f.eq_sk IS NULL
            THEN 'NO_FIELD_WORK'
        WHEN f.field_hectares <= 0
            THEN 'NO_POSITIVE_FIELD_HECTARES'
        WHEN f.field_hectares > c.hectares_op + 0.01
            THEN 'FIELD_AREA_GREATER_THAN_CONFIRMED'
        WHEN ABS(f.field_hectares - c.hectares_op) <= 0.01
         AND ABS(f.field_norm_liters - c.norm_liters) <= 0.01
            THEN 'RECONCILED'
        ELSE 'AREA_OR_NORM_MISMATCH'
    END AS reconciliation_status,
    now() AS _calculated_at
FROM mart.fact_fuel_plan_fact_by_operation_confirmed c
LEFT JOIN field_work f
  ON f.month_start = c.month_start
 AND f.eq_sk = c.eq_sk
 AND f.agr_operaciya_id = c.agr_operaciya_id;

CREATE OR REPLACE VIEW mart.v_field_month_operation_fuel_plan_fact_allocated AS
WITH field_by_key AS (
    SELECT
        date_trunc('month', l.work_date)::date AS month_start,
        e.eq_sk,
        l.agr_operaciya_id,
        l.pole_id,
        COUNT(*) AS waybill_lines,
        COUNT(DISTINCT l.put_list_doc_id) AS waybill_documents,
        SUM(COALESCE(l.gektarov, 0)) AS field_hectares,
        SUM(COALESCE(l.normativny_raskhod_gsm, 0)) AS field_norm_liters,
        SUM(COALESCE(l.labor_amount_rub, 0)) AS labor_amount_rub
    FROM staging.stg_waybill_field_link l
    JOIN raw.r1c_putevoy_list h
      ON h._id = l.put_list_doc_id
    JOIN mart.dim_equipment e
      ON e.code_1c = h.tehnika_id::text
    WHERE l.link_status = 'resolved_one_field_active'
      AND l.work_date IS NOT NULL
      AND l.pole_id IS NOT NULL
    GROUP BY
        date_trunc('month', l.work_date)::date,
        e.eq_sk,
        l.agr_operaciya_id,
        l.pole_id
),
reconciled_plan_fact AS (
    SELECT
        q.month_start,
        q.season_year,
        q.eq_sk,
        q.equipment,
        q.agr_operaciya_id,
        q.operation_name,
        q.confirmed_hectares,
        q.confirmed_allocated_fact_liters,
        q.confirmed_norm_liters,
        q.confirmed_variance_liters,
        q.confirmed_variance_pct,
        q.confirmed_fact_liters_per_ha,
        q.reliability_flag,
        q.fact_method
    FROM mart.v_field_month_operation_fuel_plan_fact_quality q
    WHERE q.reconciliation_status = 'RECONCILED'
)
SELECT
    r.month_start,
    r.season_year,
    r.eq_sk,
    r.equipment,
    r.agr_operaciya_id,
    r.operation_name,
    f.pole_id,
    f.waybill_lines,
    f.waybill_documents,
    ROUND(f.field_hectares, 4) AS hectares,
    ROUND(
        f.field_hectares / NULLIF(r.confirmed_hectares, 0),
        8
    ) AS allocation_share,
    ROUND(f.field_norm_liters, 4) AS normative_fuel_liters,
    ROUND(
        r.confirmed_allocated_fact_liters
        * f.field_hectares
        / NULLIF(r.confirmed_hectares, 0),
        4
    ) AS allocated_fact_liters,
    ROUND(
        r.confirmed_allocated_fact_liters
        * f.field_hectares
        / NULLIF(r.confirmed_hectares, 0)
        - f.field_norm_liters,
        4
    ) AS variance_liters,
    ROUND(
        f.field_norm_liters / NULLIF(f.field_hectares, 0),
        4
    ) AS normative_fuel_liters_per_ha,
    ROUND(
        r.confirmed_allocated_fact_liters
        / NULLIF(r.confirmed_hectares, 0),
        4
    ) AS allocated_fact_liters_per_ha,
    ROUND(
        (
            r.confirmed_allocated_fact_liters
            / NULLIF(r.confirmed_hectares, 0)
        )
        - (
            f.field_norm_liters / NULLIF(f.field_hectares, 0)
        ),
        4
    ) AS variance_liters_per_ha,
    ROUND(
        (
            (
                r.confirmed_allocated_fact_liters
                * f.field_hectares
                / NULLIF(r.confirmed_hectares, 0)
            )
            - f.field_norm_liters
        )
        / NULLIF(f.field_norm_liters, 0)
        * 100,
        2
    ) AS variance_pct,
    ROUND(f.labor_amount_rub, 2) AS labor_amount_rub,
    ROUND(
        f.labor_amount_rub / NULLIF(f.field_hectares, 0),
        2
    ) AS labor_rub_per_ha,
    r.reliability_flag,
    r.fact_method,
    'ALLOCATED_MONTHLY_EQUIPMENT_FUEL_BY_FIELD_HECTARES'::text
        AS field_fact_method,
    'RECONCILED'::text AS reconciliation_status,
    now() AS _calculated_at
FROM reconciled_plan_fact r
JOIN field_by_key f
  ON f.month_start = r.month_start
 AND f.eq_sk = r.eq_sk
 AND f.agr_operaciya_id = r.agr_operaciya_id
WHERE f.field_hectares > 0;

COMMENT ON VIEW mart.v_field_month_operation_fuel_plan_fact_allocated IS
    'Расчётный plan-fact ГСМ по полю/месяцу/технике/операции. Факт: месячное списание техники, распределённое по операции и затем по полю пропорционально площади. Не является прямым списанием ГСМ на поле.';

COMMENT ON VIEW mart.v_field_month_operation_fuel_plan_fact_quality IS
    'Контроль качества полевой аллокации plan-fact ГСМ. Показывает все confirmed ключи и статус сверки площади/норматива с полевыми строками ПУЛ.';

GRANT SELECT ON
    mart.v_field_month_operation_fuel_plan_fact_allocated,
    mart.v_field_month_operation_fuel_plan_fact_quality
TO datalens_ro;
