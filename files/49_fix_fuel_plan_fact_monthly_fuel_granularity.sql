-- 49_fix_fuel_plan_fact_monthly_fuel_granularity.sql
-- Исправление гранулярности фактического ГСМ в пооперационном plan-fact.
--
-- Причина:
-- mart.fact_fuel_writeoff хранит данные на уровне:
--   месяц × техника × марка ГСМ.
--
-- Раньше оно присоединялось к операциям только по:
--   месяц × техника.
--
-- Из-за этого одна операция размножалась по строкам/маркам ГСМ,
-- а норматив и площадь повторялись в каждой такой строке.
--
-- Исправление:
-- сначала агрегируем фактический расход до:
--   месяц × техника,
-- затем распределяем его между операциями по доле гектаров.

CREATE OR REPLACE VIEW mart.fact_fuel_plan_fact_by_operation AS
WITH meaningful_rows AS (
    SELECT
        de.eq_sk AS equipment_sk,
        date_trunc('month', pr.doc_date)::date AS period_month,
        COUNT(*) AS rows_meaningful,
        COUNT(*) FILTER (
            WHERE pr.obem_rabot_ga IS NOT NULL
              AND pr.obem_rabot_ga <> 0
        ) AS rows_with_area
    FROM mart.fact_putevoy_rabota pr
    JOIN mart.dim_equipment de
      ON de.code_1c = pr.tehnika_id
    WHERE NOT (
        (pr.obem_rabot_ga = 0 OR pr.obem_rabot_ga IS NULL)
        AND (pr.narabotka_moto_chas = 0 OR pr.narabotka_moto_chas IS NULL)
        AND pr.probeg_km IS NULL
    )
    GROUP BY
        de.eq_sk,
        date_trunc('month', pr.doc_date)::date
),
monthly_hectares AS (
    SELECT
        de.eq_sk AS equipment_sk,
        date_trunc('month', pr.doc_date)::date AS period_month,
        pr.agr_operaciya_id,
        SUM(pr.obem_rabot_ga) AS hectares_op
    FROM mart.fact_putevoy_rabota pr
    JOIN mart.dim_equipment de
      ON de.code_1c = pr.tehnika_id
    WHERE pr.obem_rabot_ga IS NOT NULL
      AND pr.obem_rabot_ga <> 0
    GROUP BY
        de.eq_sk,
        date_trunc('month', pr.doc_date)::date,
        pr.agr_operaciya_id
),
monthly_total AS (
    SELECT
        equipment_sk,
        period_month,
        SUM(hectares_op) AS hectares_total
    FROM monthly_hectares
    GROUP BY
        equipment_sk,
        period_month
),
monthly_fuel_fact AS (
    SELECT
        fw.period_month,
        fw.equipment_sk,
        SUM(COALESCE(fw.liters_consumed, 0)) AS liters_consumed
    FROM mart.fact_fuel_writeoff fw
    GROUP BY
        fw.period_month,
        fw.equipment_sk
),
fact_side AS (
    SELECT
        mh.period_month AS month_start,
        mh.equipment_sk AS eq_sk,
        mh.agr_operaciya_id,
        mh.hectares_op,
        ROUND(
            mff.liters_consumed
            * mh.hectares_op
            / NULLIF(mt.hectares_total, 0),
            1
        ) AS fuel_allocated_liters,
        ROUND(
            mff.liters_consumed
            / NULLIF(mt.hectares_total, 0),
            2
        ) AS fuel_liters_per_ha,
        mr.rows_meaningful,
        mr.rows_with_area,
        CASE
            WHEN mr.rows_meaningful = 0
                THEN 'НЕТ ДАННЫХ: все строки техники пустые в этом месяце'
            WHEN mr.rows_with_area < 5
                THEN 'НЕНАДЁЖНО: менее 5 документов с площадью за месяц'
            WHEN (
                mr.rows_with_area::numeric
                / NULLIF(mr.rows_meaningful, 0)::numeric
            ) < 0.5
                THEN 'НЕНАДЁЖНО: >50% значимых строк без площади'
            ELSE 'OK'
        END AS reliability_flag
    FROM monthly_hectares mh
    JOIN monthly_total mt
      USING (equipment_sk, period_month)
    JOIN meaningful_rows mr
      USING (equipment_sk, period_month)
    LEFT JOIN monthly_fuel_fact mff
      ON mff.equipment_sk = mh.equipment_sk
     AND mff.period_month = mh.period_month
),
norm_side AS (
    SELECT
        date_trunc('month', l.den_raboty)::date AS month_start,
        eq.eq_sk,
        l.agr_operaciya_id,
        SUM(l.normativny_raskhod_gsm) AS norm_liters
    FROM raw.r1c_putevoy_list_lines l
    JOIN raw.r1c_putevoy_list doc
      ON doc._id = l.doc_id
    LEFT JOIN mart.dim_equipment eq
      ON eq.code_1c = doc.tehnika_id
    WHERE doc._posted = true
      AND COALESCE(doc._deletionmark, false) = false
      AND l.normativny_raskhod_gsm IS NOT NULL
      AND l.den_raboty IS NOT NULL
      AND l.agr_operaciya_id IS NOT NULL
    GROUP BY
        date_trunc('month', l.den_raboty)::date,
        eq.eq_sk,
        l.agr_operaciya_id
)
SELECT
    COALESCE(f.month_start, n.month_start) AS month_start,
    COALESCE(f.eq_sk, n.eq_sk) AS eq_sk,
    de.name AS equipment,
    COALESCE(f.agr_operaciya_id, n.agr_operaciya_id)
        AS agr_operaciya_id,
    op.description AS operation_name,
    f.hectares_op,
    COALESCE(f.fuel_allocated_liters, 0) AS fact_liters,
    COALESCE(n.norm_liters, 0) AS norm_liters,
    COALESCE(f.fuel_allocated_liters, 0)
        - COALESCE(n.norm_liters, 0) AS variance_liters,
    CASE
        WHEN COALESCE(n.norm_liters, 0) > 0
            THEN ROUND(
                (
                    COALESCE(f.fuel_allocated_liters, 0)
                    - n.norm_liters
                )
                / n.norm_liters
                * 100,
                1
            )
        ELSE NULL
    END AS variance_pct,
    f.fuel_liters_per_ha,
    COALESCE(
        f.reliability_flag,
        'НЕТ ДАННЫХ: нет фактической аллокации по операции'
    ) AS reliability_flag
FROM fact_side f
FULL JOIN norm_side n
  ON n.month_start = f.month_start
 AND n.eq_sk = f.eq_sk
 AND n.agr_operaciya_id = f.agr_operaciya_id
LEFT JOIN mart.dim_equipment de
  ON de.eq_sk = COALESCE(f.eq_sk, n.eq_sk)
LEFT JOIN raw.r1c_tech_operations op
  ON op._id = COALESCE(f.agr_operaciya_id, n.agr_operaciya_id);

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
    'ALLOCATED_MONTHLY_EQUIPMENT_FUEL_BY_HECTARES'::text AS fact_method
FROM mart.fact_fuel_plan_fact_by_operation
WHERE reliability_flag = 'OK'
  AND hectares_op > 0
  AND norm_liters > 0
  AND fact_liters > 0;

COMMENT ON VIEW mart.fact_fuel_plan_fact_by_operation IS
    'План-факт ГСМ по операции. Факт — месячное списание техники, предварительно агрегированное по месяцу и технике и распределённое по операциям пропорционально площади.';

COMMENT ON VIEW mart.fact_fuel_plan_fact_by_operation_confirmed IS
    'Качественный набор пооперационного plan-fact ГСМ. Факт распределён по площади, не является прямым списанием на операцию.';

GRANT SELECT ON
    mart.fact_fuel_plan_fact_by_operation,
    mart.fact_fuel_plan_fact_by_operation_reliable,
    mart.fact_fuel_plan_fact_by_operation_confirmed
TO datalens_ro;
