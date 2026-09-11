-- ============================================================================
-- mart.v_fuel_norm_by_operation (v5) -- фикс на абсолютное число документов
--
-- НАЙДЕННАЯ ОШИБКА v4 (сессия 2026-09-11):
-- Индикатор reliability_flag использовал ТОЛЬКО долю rows_with_area_share_pct.
-- На случае 'Lovol 2604, Гос.№9130ХХ' за 2026-06: rows_meaningful=1,
-- rows_with_area=1 -> 100.0% -> 'OK', хотя вывод построен на ОДНОМ документе.
-- Пропорция 100% на выборке из 1 строки статистически ничего не значит.
--
-- ИСПРАВЛЕНО: добавлено условие на абсолютное количество документов с
-- площадью (rows_with_area >= 5) вдобавок к доле (>= 50%). Оба условия
-- должны выполняться одновременно, иначе flag = НЕНАДЁЖНО.
--
-- ПОРОГ N=5 выбран по анализу чувствительности (см.
-- 26_check_threshold_sensitivity.sql и SESSION_2026-09-11): ok_avg_l_ga
-- выходит на плато начиная с N=5 (19.2 -> 15.6 -> 14.5 -> 14.0 для
-- N=3,5,7,10), дальнейшее ужесточение режет выборку без соразмерного
-- выигрыша в точности.
-- ============================================================================

CREATE OR REPLACE VIEW mart.v_fuel_norm_by_operation AS
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
)
SELECT
    mh.period_month,
    de.name AS equipment,
    mh.agr_operaciya_id,
    mh.hectares_op,
    mt.hectares_total,
    fw.liters_consumed AS fuel_month_total,
    ROUND(fw.liters_consumed * (mh.hectares_op / mt.hectares_total), 1) AS fuel_allocated_liters,
    ROUND((fw.liters_consumed * (mh.hectares_op / mt.hectares_total)) / mh.hectares_op, 2) AS fuel_liters_per_ha,
    mr.rows_meaningful,
    mr.rows_with_area,
    ROUND(100.0 * mr.rows_with_area / NULLIF(mr.rows_meaningful, 0), 1) AS rows_with_area_share_pct,
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
JOIN mart.dim_equipment de ON de.eq_sk = mh.equipment_sk
LEFT JOIN mart.fact_fuel_writeoff fw
    ON fw.equipment_sk = mh.equipment_sk AND fw.period_month = mh.period_month
ORDER BY mh.period_month DESC, equipment, fuel_liters_per_ha DESC;

CREATE OR REPLACE VIEW mart.v_fuel_norm_by_operation_reliable AS
SELECT * FROM mart.v_fuel_norm_by_operation
WHERE reliability_flag = 'OK';
