-- 28_mart_fuel_plan_fact_by_operation.sql (fix: eq_sk недоступен в v_fuel_norm_by_operation)
-- Пооперационный план-факт ГСМ (л/га) по технике/операции/месяцу.
-- Переиспользует CTE-логику из 25_view_fuel_norm_by_operation_v5.sql напрямую
-- (а не через готовую mart.v_fuel_norm_by_operation), т.к. та view не отдаёт
-- equipment_sk наружу -- джойнить по текстовому имени техники ненадёжно.

CREATE OR REPLACE VIEW mart.fact_fuel_plan_fact_by_operation AS
WITH meaningful_rows AS (
    SELECT
        de.eq_sk AS equipment_sk,
        date_trunc('month', pr.doc_date)::date AS period_month,
        COUNT(*) AS rows_meaningful,
        COUNT(*) FILTER (WHERE pr.obem_rabot_ga IS NOT NULL AND pr.obem_rabot_ga <> 0) AS rows_with_area
    FROM mart.fact_putevoy_rabota pr
    JOIN mart.dim_equipment de ON de.code_1c = pr.tehnika_id
    WHERE NOT (
        (pr.obem_rabot_ga = 0 OR pr.obem_rabot_ga IS NULL)
        AND (pr.narabotka_moto_chas = 0 OR pr.narabotka_moto_chas IS NULL)
        AND pr.probeg_km IS NULL
    )
    GROUP BY de.eq_sk, date_trunc('month', pr.doc_date)
),
monthly_hectares AS (
    SELECT
        de.eq_sk AS equipment_sk,
        date_trunc('month', pr.doc_date)::date AS period_month,
        pr.agr_operaciya_id,
        SUM(pr.obem_rabot_ga) AS hectares_op
    FROM mart.fact_putevoy_rabota pr
    JOIN mart.dim_equipment de ON de.code_1c = pr.tehnika_id
    WHERE pr.obem_rabot_ga IS NOT NULL AND pr.obem_rabot_ga <> 0
    GROUP BY de.eq_sk, date_trunc('month', pr.doc_date), pr.agr_operaciya_id
),
monthly_total AS (
    SELECT equipment_sk, period_month, SUM(hectares_op) AS hectares_total
    FROM monthly_hectares
    GROUP BY equipment_sk, period_month
),
fact_side AS (
    SELECT
        mh.period_month AS month_start,
        mh.equipment_sk AS eq_sk,
        mh.agr_operaciya_id,
        mh.hectares_op,
        ROUND(fw.liters_consumed * (mh.hectares_op / mt.hectares_total), 1) AS fuel_allocated_liters,
        ROUND((fw.liters_consumed * (mh.hectares_op / mt.hectares_total)) / mh.hectares_op, 2) AS fuel_liters_per_ha,
        mr.rows_meaningful,
        mr.rows_with_area,
        CASE
            WHEN mr.rows_meaningful = 0 THEN 'НЕТ ДАННЫХ: все строки техники пустые в этом месяце'
            WHEN mr.rows_with_area < 5 THEN 'НЕНАДЁЖНО: менее 5 документов с площадью за месяц'
            WHEN mr.rows_with_area::numeric / NULLIF(mr.rows_meaningful, 0) < 0.5
                THEN 'НЕНАДЁЖНО: >50% значимых строк без площади'
            ELSE 'OK'
        END AS reliability_flag
    FROM monthly_hectares mh
    JOIN monthly_total mt USING (equipment_sk, period_month)
    JOIN meaningful_rows mr USING (equipment_sk, period_month)
    LEFT JOIN mart.fact_fuel_writeoff fw
        ON fw.equipment_sk = mh.equipment_sk AND fw.period_month = mh.period_month
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
      AND (doc._deletionmark IS NULL OR doc._deletionmark = false)
      AND l.normativny_raskhod_gsm IS NOT NULL
      AND l.den_raboty IS NOT NULL
      AND l.agr_operaciya_id IS NOT NULL
    GROUP BY 1, 2, 3
)
SELECT
    COALESCE(f.month_start, n.month_start)      AS month_start,
    COALESCE(f.eq_sk, n.eq_sk)                  AS eq_sk,
    de.name                                     AS equipment,
    COALESCE(f.agr_operaciya_id, n.agr_operaciya_id) AS agr_operaciya_id,
    op.description                              AS operation_name,
    f.hectares_op,
    COALESCE(f.fuel_allocated_liters, 0)        AS fact_liters,
    COALESCE(n.norm_liters, 0)                  AS norm_liters,
    COALESCE(f.fuel_allocated_liters, 0) - COALESCE(n.norm_liters, 0) AS variance_liters,
    CASE WHEN COALESCE(n.norm_liters, 0) > 0
         THEN ROUND((COALESCE(f.fuel_allocated_liters, 0) - n.norm_liters) / n.norm_liters * 100, 1)
         ELSE NULL
    END                                          AS variance_pct,
    f.fuel_liters_per_ha,
    COALESCE(f.reliability_flag, 'НЕТ ДАННЫХ: нет фактической аллокации по операции') AS reliability_flag
FROM fact_side f
FULL OUTER JOIN norm_side n
    ON n.month_start = f.month_start
   AND n.eq_sk = f.eq_sk
   AND n.agr_operaciya_id = f.agr_operaciya_id
LEFT JOIN mart.dim_equipment de
    ON de.eq_sk = COALESCE(f.eq_sk, n.eq_sk)
LEFT JOIN raw.r1c_tech_operations op
    ON op._id = COALESCE(f.agr_operaciya_id, n.agr_operaciya_id);

COMMENT ON VIEW mart.fact_fuel_plan_fact_by_operation IS
    'Пооперационный план-факт ГСМ (л и л/га) по технике/операции/месяцу. '
    'Норма из putevoy_list_lines.normativny_raskhod_gsm, факт -- аллоцированный '
    'по площади (та же логика, что в v_fuel_norm_by_operation v5, но с eq_sk наружу). '
    'Использовать только reliability_flag=OK. Создано 2026-09-15.';

CREATE OR REPLACE VIEW mart.fact_fuel_plan_fact_by_operation_reliable AS
SELECT * FROM mart.fact_fuel_plan_fact_by_operation
WHERE reliability_flag = 'OK';

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'datalens_ro') THEN
        EXECUTE 'GRANT SELECT ON mart.fact_fuel_plan_fact_by_operation TO datalens_ro';
        EXECUTE 'GRANT SELECT ON mart.fact_fuel_plan_fact_by_operation_reliable TO datalens_ro';
    END IF;
END $$;
