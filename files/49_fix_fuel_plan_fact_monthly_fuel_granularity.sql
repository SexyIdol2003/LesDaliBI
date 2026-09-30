-- KPI 5: расчётное распределение месячного списания ГСМ по операциям.
-- Источник факта: месяц × техника × марка ГСМ.
-- Сначала суммируем марки до месяца × техники, затем распределяем
-- по положительному нормативу всех операций, включая работы без площади.
-- При отсутствии положительного норматива факт остаётся отдельной
-- нераспределённой строкой. Это НЕ прямой факт расхода по операции.

CREATE OR REPLACE VIEW mart.fact_fuel_plan_fact_by_operation AS
WITH operation_rows AS (
    SELECT
        date_trunc('month', l.den_raboty)::date AS month_start,
        eq.eq_sk,
        l.agr_operaciya_id,
        SUM(COALESCE(l.gektarov, 0)) AS hectares_op,
        SUM(
            GREATEST(COALESCE(l.normativny_raskhod_gsm, 0), 0)
        ) AS positive_norm_liters,
        SUM(COALESCE(l.normativny_raskhod_gsm, 0)) AS norm_liters,
        COUNT(*) AS work_rows,
        COUNT(*) FILTER (
            WHERE COALESCE(l.gektarov, 0) > 0
        ) AS rows_with_area
    FROM raw.r1c_putevoy_list_lines l
    JOIN raw.r1c_putevoy_list doc
      ON doc._id = l.doc_id
    JOIN mart.dim_equipment eq
      ON eq.code_1c = doc.tehnika_id
    WHERE doc._posted = true
      AND COALESCE(doc._deletionmark, false) = false
      AND l.den_raboty IS NOT NULL
      AND l.agr_operaciya_id IS NOT NULL
    GROUP BY 1, 2, 3
),
weights AS (
    SELECT
        o.*,
        SUM(o.positive_norm_liters) OVER (
            PARTITION BY o.month_start, o.eq_sk
        ) AS month_positive_norm_liters,
        SUM(o.work_rows) OVER (
            PARTITION BY o.month_start, o.eq_sk
        ) AS month_work_rows,
        SUM(o.rows_with_area) OVER (
            PARTITION BY o.month_start, o.eq_sk
        ) AS month_rows_with_area
    FROM operation_rows o
),
monthly_fact AS (
    SELECT
        fw.period_month AS month_start,
        fw.equipment_sk AS eq_sk,
        SUM(COALESCE(fw.liters_consumed, 0)) AS monthly_liters
    FROM mart.fact_fuel_writeoff fw
    GROUP BY 1, 2
),
allocated AS (
    SELECT
        w.month_start,
        w.eq_sk,
        w.agr_operaciya_id,
        NULLIF(w.hectares_op, 0) AS hectares_op,
        CASE
            WHEN w.positive_norm_liters > 0
             AND w.month_positive_norm_liters > 0
            THEN f.monthly_liters * w.positive_norm_liters
                 / w.month_positive_norm_liters
            ELSE 0::numeric
        END AS fact_liters,
        w.norm_liters,
        CASE
            WHEN f.eq_sk IS NULL THEN
                'НЕТ ДАННЫХ: нет месячного списания'
            WHEN w.month_positive_norm_liters <= 0 THEN
                'НЕТ ДАННЫХ: нет положительного норматива'
            WHEN w.positive_norm_liters <= 0 THEN
                'НЕТ ДАННЫХ: операция без положительного норматива'
            WHEN w.month_rows_with_area < 5 THEN
                'НЕНАДЁЖНО: менее 5 строк с площадью за месяц'
            WHEN w.month_rows_with_area::numeric
                 / NULLIF(w.month_work_rows, 0) < 0.5 THEN
                'НЕНАДЁЖНО: >50% строк без площади'
            ELSE 'OK'
        END AS reliability_flag
    FROM weights w
    LEFT JOIN monthly_fact f
      ON f.month_start = w.month_start
     AND f.eq_sk = w.eq_sk
),
unallocated AS (
    SELECT
        f.month_start,
        f.eq_sk,
        NULL::text AS agr_operaciya_id,
        NULL::numeric AS hectares_op,
        f.monthly_liters AS fact_liters,
        0::numeric AS norm_liters,
        'НЕТ ДАННЫХ: месячный факт не распределён — нет положительного норматива'
            ::text AS reliability_flag
    FROM monthly_fact f
    LEFT JOIN (
        SELECT DISTINCT
            month_start,
            eq_sk,
            month_positive_norm_liters
        FROM weights
    ) w
      ON w.month_start = f.month_start
     AND w.eq_sk = f.eq_sk
    WHERE f.monthly_liters <> 0
      AND COALESCE(w.month_positive_norm_liters, 0) <= 0
),
combined AS (
    SELECT * FROM allocated
    UNION ALL
    SELECT * FROM unallocated
)
SELECT
    c.month_start,
    c.eq_sk,
    de.name AS equipment,
    c.agr_operaciya_id,
    op.description AS operation_name,
    c.hectares_op,
    c.fact_liters,
    c.norm_liters,
    c.fact_liters - c.norm_liters AS variance_liters,
    CASE
        WHEN c.norm_liters > 0
        THEN ROUND(
            (c.fact_liters - c.norm_liters)
            / c.norm_liters * 100,
            1
        )
        ELSE NULL::numeric
    END AS variance_pct,
    CASE
        WHEN c.hectares_op > 0
        THEN ROUND(c.fact_liters / c.hectares_op, 2)
        ELSE NULL::numeric
    END AS fuel_liters_per_ha,
    c.reliability_flag
FROM combined c
LEFT JOIN mart.dim_equipment de
  ON de.eq_sk = c.eq_sk
LEFT JOIN raw.r1c_tech_operations op
  ON op._id = c.agr_operaciya_id;

CREATE OR REPLACE VIEW mart.fact_fuel_plan_fact_by_operation_reliable AS
SELECT
    month_start,
    eq_sk,
    equipment,
    agr_operaciya_id,
    operation_name,
    hectares_op,
    fact_liters,
    norm_liters,
    variance_liters,
    variance_pct,
    fuel_liters_per_ha,
    reliability_flag
FROM mart.fact_fuel_plan_fact_by_operation
WHERE reliability_flag = 'OK';

CREATE OR REPLACE VIEW mart.fact_fuel_plan_fact_by_operation_confirmed AS
SELECT
    month_start,
    eq_sk,
    equipment,
    agr_operaciya_id,
    operation_name,
    hectares_op,
    fact_liters,
    norm_liters,
    variance_liters,
    variance_pct,
    fuel_liters_per_ha,
    reliability_flag,
    'ALLOCATED_MONTHLY_EQUIPMENT_FUEL_BY_POSITIVE_NORM'::text
        AS fact_method
FROM mart.fact_fuel_plan_fact_by_operation
WHERE hectares_op > 0
  AND norm_liters > 0
  AND fact_liters > 0
  AND reliability_flag NOT LIKE 'НЕТ ДАННЫХ:%';

COMMENT ON VIEW mart.fact_fuel_plan_fact_by_operation IS
    'KPI 5: месячное списание техники распределено по положительному нормативу операций. Это расчётная аллокация, не прямой факт по операции; при отсутствии норматива факт показан отдельной нераспределённой строкой.';

COMMENT ON VIEW mart.fact_fuel_plan_fact_by_operation_confirmed IS
    'Кандидаты для полевой сверки: расчётная аллокация с площадью и нормативом. Статус месячного покрытия строк сохранён как диагностика; окончательное полевое подтверждение требует reconciliation_status = RECONCILED в витрине №50. Не является прямым списанием по операции.';

GRANT SELECT ON
    mart.fact_fuel_plan_fact_by_operation,
    mart.fact_fuel_plan_fact_by_operation_reliable,
    mart.fact_fuel_plan_fact_by_operation_confirmed
TO datalens_ro;
