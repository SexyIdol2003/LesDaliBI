-- 34_harvest_yield_variety_ready.sql
-- Сессия 2026-09-21: отфильтрованная версия витрины урожайности по сортам
-- для подключения в DataLens (без строк с NULL yield_t_ha, вызванных
-- отсутствием площади по сорту за 2023-2024 годы).

CREATE OR REPLACE VIEW mart.v_fact_harvest_yield_by_variety_ready AS
SELECT * FROM mart.v_fact_harvest_yield_by_variety
WHERE yield_t_ha IS NOT NULL;

GRANT SELECT ON mart.v_fact_harvest_yield_by_variety_ready TO datalens_ro;
