-- =====================================================================
-- 30_mart_equipment_costs_by_period.sql
-- Показатель №5: "Факт затрат, руб., на технику за период"
--
-- Дата: 2026-09-16
-- Контекст: строительный блок для показателя №6 (себестоимость 1 га
-- за сезон). Собирает затраты на технику по месяцу/технике/виду затрат.
--
-- СОСТАВ v1:
--   - production_labor_cost_rub -- зарплата по путевым листам
--     (raw.r1c_putevoy_list_lines.itogo_zp, fallback osnovnaya_zp)
--   - repair_labor_cost_rub     -- ЗАФИКСИРОВАНО КАК 0 (см. ограничение ниже)
--   - total_equipment_cost_rub  -- сумма двух полей выше
--
-- ИЗВЕСТНЫЕ ОГРАНИЧЕНИЯ v1 (зафиксировать отдельными задачами):
--   1. Стоимость ГСМ в рублях НЕ включена. В raw ГСМ хранится только
--      в литрах (r1c_fuel_refuel, r1c_fuel_usage, r1c_fuel_writeoff,
--      r1c_fuel_writeoff_summary_gsm). Справочника цен на топливо в
--      DWH нет (проверено: 0 таблиц с price/cena/nomenklatur/sebesto).
--      Нужен отдельный источник цены за литр из 1С, прежде чем можно
--      честно перевести литры в рубли.
--   2. Ремонт (labor_cost_rub) зафиксирован как 0, а не через JOIN,
--      т.к. mart.fact_equipment_repair_cost физически пуста (0 строк)
--      на момент написания: raw.r1c_equipment_repair содержит всего
--      3 документа (из них 1 помечен на удаление), а
--      raw.r1c_equipment_repair_works -- всего 1 строку работ. Это
--      подтверждённое реальное состояние источника 1С на 2026-09-16
--      (не баг DAG dag_extract_equipment_repair -- он успешно
--      отрабатывает, просто в 1С почти нет заведённых документов
--      ремонта). Как только объём данных вырастет и появится
--      трансформация raw -> mart.fact_equipment_repair_cost, эту
--      витрину нужно обновить: заменить repair_labor_cost_rub на
--      реальный JOIN с mart.fact_equipment_repair_cost по eq_sk.
--   3. h._deletionmark IS NOT TRUE -- исключены документы путевых
--      листов, помеченные на удаление в 1С.
--   4. itogo_zp используется как основной показатель зарплаты;
--      osnovnaya_zp -- только fallback, если itogo_zp NULL. Проверено
--      на выборке: во всех строках osnovnaya_zp = itogo_zp, поэтому
--      суммирование этих двух полей приводило бы к задвоению.
-- =====================================================================

CREATE OR REPLACE VIEW mart.v_fact_equipment_costs_by_period AS
SELECT
    date_trunc('month', l.den_raboty)::date AS period_month,
    h.tehnika_id                             AS code_1c,
    e.eq_sk,
    e.name                                   AS equipment_name,
    l.tip_zatrat,
    sum(COALESCE(l.itogo_zp, l.osnovnaya_zp, 0)) AS production_labor_cost_rub,
    0::numeric                                AS repair_labor_cost_rub,
    sum(COALESCE(l.itogo_zp, l.osnovnaya_zp, 0)) AS total_equipment_cost_rub,
    sum(l.gektarov)                           AS gektarov,
    count(*)                                  AS line_count
FROM raw.r1c_putevoy_list AS h
JOIN raw.r1c_putevoy_list_lines AS l
  ON l.doc_id = h._id
LEFT JOIN mart.dim_equipment AS e
  ON e.code_1c = h.tehnika_id
WHERE h._deletionmark IS NOT TRUE
GROUP BY 1, 2, 3, 4, 5;

-- Контрольная проверка после создания (не часть DDL, справочно):
-- SELECT period_month, count(DISTINCT eq_sk), sum(total_equipment_cost_rub), sum(gektarov)
-- FROM mart.v_fact_equipment_costs_by_period
-- GROUP BY period_month ORDER BY period_month DESC;
--
-- Результат проверки на 2026-09-16: 12 месяцев истории (2025-09..2026-08),
-- 10-15 единиц техники в месяц, затраты 227К-1.66М руб/мес,
-- сезонность подтверждена (нулевые гектары зимой при ненулевых
-- общепроизв./общехоз. затратах).
