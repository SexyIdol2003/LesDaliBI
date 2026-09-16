-- ============================================================================
-- 29_mart_fuel_cost_by_equipment_month.sql
-- Расчётная стоимость фактического расхода ГСМ по технике и месяцам.
--
-- Правило:
--   Литры: mart.fact_fuel_writeoff.liters_consumed
--          (1С: «Списание топлива по суммарной заправке»).
--   Цена:  mart.dim_fuel_price_monthly.price_rub_per_liter
--          (ручная учётная цена по отчёту 1С).
--   Сумма: liters_consumed * price_rub_per_liter.
--
-- Май 2026:
--   27 415,690 л × 53,60 руб./л = 1 469 480,98 руб.
--
-- Важно:
--   fuel_cost_rub — расчётная оценка для BI, а не прямая бухгалтерская
--   сумма списания со склада.
-- ============================================================================

BEGIN;

INSERT INTO mart.dim_fuel_price_monthly (
    period_month,
    fuel_brand_id,
    fuel_name,
    price_rub_per_liter,
    source_liters,
    source_cost_rub,
    source_description
)
VALUES (
    DATE '2026-05-01',
    '13d3b0a9-30ed-11ec-b9aa-00505689914b',
    'Дизельное топливо — учётная цена',
    53.60,
    850.897,
    45608.09,
    'Ручная учётная цена по отчёту 1С «Себестоимость товаров организаций»: ДТ Евро сорт С; цена = 45 608,09 / 850,897 = 53,60 руб./л. Для BI применяется к фактическому расходу по документу «Списание топлива по суммарной заправке».'
)
ON CONFLICT (period_month, fuel_brand_id)
DO UPDATE SET
    fuel_name = EXCLUDED.fuel_name,
    price_rub_per_liter = EXCLUDED.price_rub_per_liter,
    source_liters = EXCLUDED.source_liters,
    source_cost_rub = EXCLUDED.source_cost_rub,
    source_description = EXCLUDED.source_description,
    _loaded_at = now();

CREATE OR REPLACE VIEW mart.v_fuel_cost_by_equipment_month AS
SELECT
    fw.period_month,
    fw.equipment_sk,
    eq.name AS equipment_name,
    fw.fuel_brand_id,
    COALESCE(p.fuel_name, 'Цена не задана') AS fuel_name,
    fw.liters_start,
    fw.liters_refueled,
    fw.liters_consumed,
    fw.liters_end,
    p.price_rub_per_liter,
    ROUND(fw.liters_consumed * p.price_rub_per_liter, 2) AS fuel_cost_rub,
    p.source_description AS price_source,
    CASE
        WHEN p.price_rub_per_liter IS NULL THEN 'НЕТ ЦЕНЫ'
        ELSE 'OK'
    END AS price_status,
    fw.src_doc_ref
FROM mart.fact_fuel_writeoff AS fw
LEFT JOIN mart.dim_equipment AS eq
    ON eq.eq_sk = fw.equipment_sk
LEFT JOIN mart.dim_fuel_price_monthly AS p
    ON p.period_month = fw.period_month
   AND p.fuel_brand_id = fw.fuel_brand_id;

COMMIT;

-- Контроль после применения:
-- SELECT
--     period_month,
--     fuel_name,
--     price_rub_per_liter,
--     ROUND(SUM(liters_consumed), 3) AS fuel_liters,
--     ROUND(SUM(fuel_cost_rub), 2) AS fuel_cost_rub,
--     COUNT(DISTINCT equipment_sk) AS equipment_cnt,
--     MIN(price_status) AS price_status
-- FROM mart.v_fuel_cost_by_equipment_month
-- WHERE period_month = DATE '2026-05-01'
-- GROUP BY period_month, fuel_name, price_rub_per_liter;
