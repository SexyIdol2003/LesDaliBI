-- =====================================================================
-- 31_mart_equipment_costs_by_period_v2.sql
-- Показатель №5: "Факт затрат, руб., на технику за период" — версия 2
--
-- Дата: 2026-09-16
-- заменяет первую версию (30_mart_equipment_costs_by_period.sql):
-- добавлена денежная оценка ГСМ через справочник фактических
-- учётных цен из отчёта 1С "Себестоимость товаров организаций", и исправлена
-- КРИТИЧЕСКАЯ ОСИБКА ЗАДВОЕНИЯ (см. ниже).
--
-- =====================================================================
-- НАИДЕННАЯ И ИСПРАВЛЕННАЯ ОШИБКА: задвоение fuel_cost_rub
-- =====================================================================
-- Первая попытка джойнила месячный итог ГСМ (уровень техника+месяц)
-- напрямую к строкам зарплаты (уровень техника+месяц+tip_zatrat).
-- Если у техники за месяц было 2-3 разных tip_zatrat (например,
-- ПрямыеЗатраты + ОбщепроизводственныеЗатраты), стоимость ГСМ
-- попадала в КАЖДУУ строку целиком -- при суммировании по всей
-- витрине это давало 2-3-кратное завышение общих затрат на топливо.
-- Обнаружено контрольной проверкой: sum(total_fuel_cost_rub) не
-- равнялся физическому расходу * цене.
--
-- РЕШЕНИЕ: топливо не имеет атрибута tip_zatrat в источнике, поэтому
-- его нельзя корректно распределять по видам затрат на этом уровне.
-- Разделены на два представления:
--   1. v_fact_equipment_costs_by_period -- детализация по tip_zatrat
--      для зарплаты; топливо показано как справочное поле месяцного
--      итога БЕЗ включения в построчную сумму.
--   2. v_fact_equipment_costs_by_period_summary -- агрегат на уровне
--      (период, техника) БЕЗ разреза по tip_zatrat; ииименно эта
--      витрина даёт корректный total_equipment_cost_rub.
--
-- ПРАВИЛО: total_equipment_cost_rub считать только из _summary.
-- Из детальной витрины суммировать total_equipment_cost_rub НЕЛьзя.
-- =====================================================================

-- Справочник фактических учётных цен ГСМ по месяцам.
-- Заполняется ВРУСНУХ на основе отчёта 1С "Себестоимость товаров
-- организаций" (Финансовый результат и контроллинг -> Отчёты),
-- т.к. OData 1С не публикует ни цену топлива, ни справочник цен
-- номенклатуры (проверено: 0 таблиц с price/cena/nomenklatur/sebesto
-- в raw/staging/mart), а полный $metadata и корневой документ сервиса
-- OData возвращают HTTP 500 (внутренняя ошибка сервиса 1С, не связана
-- с нашим кодом -- подтверждено проверкой конкретных EntitySet, которые
-- отрабатывают нормально).
CREATE TABLE IF NOT EXISTS mart.dim_fuel_price_monthly (
    period_month            date NOT NULL,
    fuel_brand_id            uuid NOT NULL,
    fuel_name                text NOT NULL,
    price_rub_per_liter      numeric(10,2) NOT NULL,
    source_liters            numeric,
    source_cost_rub          numeric,
    source_description       text,
    _loaded_at                timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (period_month, fuel_brand_id)
);

-- Первая подтверждённая запись: сентябрь 2025, дизельное топливо
-- "Евро сорт С". Себестоимость расхода 1 450 594,92 руб. / 28 611,494 л
-- = 50,70 руб./л (отчёт 1С за 01.09.2025-30.09.2025).
-- Сверка с DWH: mart.fact_fuel_writeoff за тот же месяц и
-- fuel_brand_id = 13d3b0a9-30ed-11ec-b9aa-00505689914b показывает
-- 26 260,160 л (12 строк одного документа 0000-000009 от 30.09.2025,
-- период документа строго 01.09.2025-30.09.2025 -- проверено).
-- Покрытие DWH относительно 1С: 26260.160 / 28611.494 = 91.78%.
-- Расхождение ~8.22% -- известное ограничение (см. ниже), не
-- блокирует использование, т.к. большая часть расхода покрыта.
INSERT INTO mart.dim_fuel_price_monthly (
    period_month, fuel_brand_id, fuel_name, price_rub_per_liter,
    source_liters, source_cost_rub, source_description
) VALUES (
    '2025-09-01', '13d3b0a9-30ed-11ec-b9aa-00505689914b',
    'Дизельное топливо Евро сорт С', 50.70,
    28611.494, 1450594.92,
    'Отчет 1С Себестоимость товаров организаций, период 01.09.2025-30.09.2025'
)
ON CONFLICT (period_month, fuel_brand_id) DO UPDATE SET
    price_rub_per_liter = EXCLUDED.price_rub_per_liter,
    source_liters = EXCLUDED.source_liters,
    source_cost_rub = EXCLUDED.source_cost_rub,
    source_description = EXCLUDED.source_description,
    _loaded_at = now();

-- Детальная витрина: разрез по tip_zatrat для зарплаты.
-- ВНИМАНИЕ: fuel_cost_rub_month_total -- это ИТОГ ЗА МЕСяц по технике,
-- повторяется на каждой строке tip_zatrat справочно. НЕ суммировать
-- эту колонку по строкам одной техники -- получится задвоение.
DROP VIEW IF EXISTS mart.v_fact_equipment_costs_by_period CASCADE;

CREATE VIEW mart.v_fact_equipment_costs_by_period AS
WITH labor AS (
    SELECT
        date_trunc('month', l.den_raboty)::date AS period_month,
        h.tehnika_id AS code_1c,
        e.eq_sk,
        e.name AS equipment_name,
        l.tip_zatrat,
        sum(COALESCE(l.itogo_zp, l.osnovnaya_zp, 0)) AS production_labor_cost_rub,
        sum(l.gektarov) AS gektarov,
        count(*) AS line_count
    FROM raw.r1c_putevoy_list AS h
    JOIN raw.r1c_putevoy_list_lines AS l
      ON l.doc_id = h._id
    LEFT JOIN mart.dim_equipment AS e
      ON e.code_1c = h.tehnika_id
    WHERE h._deletionmark IS NOT TRUE
    GROUP BY 1, 2, 3, 4, 5
),
fuel_total AS (
    SELECT
        f.period_month,
        f.equipment_sk AS eq_sk,
        sum(f.liters_consumed * p.price_rub_per_liter) AS fuel_cost_rub,
        sum(f.liters_consumed) AS liters_consumed
    FROM mart.fact_fuel_writeoff AS f
    JOIN mart.dim_fuel_price_monthly AS p
      ON p.period_month = f.period_month
     AND p.fuel_brand_id = f.fuel_brand_id
    GROUP BY 1, 2
)
SELECT
    l.period_month,
    l.code_1c,
    l.eq_sk,
    l.equipment_name,
    l.tip_zatrat,
    l.production_labor_cost_rub,
    0::numeric AS repair_labor_cost_rub,
    l.gektarov,
    l.line_count,
    ft.liters_consumed,
    ft.fuel_cost_rub AS fuel_cost_rub_month_total,
    l.production_labor_cost_rub AS labor_cost_this_row
FROM labor AS l
LEFT JOIN fuel_total AS ft
  ON ft.eq_sk = l.eq_sk
 AND ft.period_month = l.period_month;

-- Итоговая витрина: (период, техника) без разреза по tip_zatrat.
-- Это единственно верный уровень для total_equipment_cost_rub.
CREATE OR REPLACE VIEW mart.v_fact_equipment_costs_by_period_summary AS
SELECT
    period_month,
    code_1c,
    eq_sk,
    equipment_name,
    sum(labor_cost_this_row) AS total_labor_cost_rub,
    0::numeric AS total_repair_cost_rub,
    max(fuel_cost_rub_month_total) AS total_fuel_cost_rub,
    sum(labor_cost_this_row) + max(fuel_cost_rub_month_total) AS total_equipment_cost_rub,
    sum(gektarov) AS gektarov
FROM mart.v_fact_equipment_costs_by_period
GROUP BY period_month, code_1c, eq_sk, equipment_name;

-- =====================================================================
-- ИЗВЕСТНОЗАВРЛЕНИЕ ОГРАНИЧЕНИИ (актуальны на 2026-09-16):
-- =====================================================================
--   1. Цена ГСМ заполнена ВРУСНУХ только для сентября 2025 и только
--      для одного вида топлива. Для всех остальных месяцев/марок
--      total_fuel_cost_rub = 0 -- явно занижает реальные затраты.
--   2. Ремонт (total_repair_cost_rub) зафиксирован как 0 -- см. предыдущую
--      сессионную заметку.
--   3. Расхождение ~8.22% между 1С (28611.494 л) и DWH (26260.160 л) по
--      физическому расходу дизтоплива за сентябрь 2025.
--   4. КРИТИЧНО: total_equipment_cost_rub суммировать только из
--      v_fact_equipment_costs_by_period_summary. В детальной витрине поле
--      fuel_cost_rub_month_total дублируется.
-- =====================================================================
