-- REVERTED 2026-09-07: mart.fact_fuel_usage и mart.fact_fuel_refuel уже существуют
-- как таблицы со звездными ключами (mart.dim_equipment, mart.dim_date, mart.dim_field)
-- в рамках 03_mart.sql / 05_mart_facts.sql. Попытка создать одноименные VIEW
-- выше конфликтовала с реальными таблицами ("is not a view").
-- Правильная загрузка данных вынесена в files/17_mart_fact_fuel_refuel_load.sql.
-- Файл оставлен как пустой для истории миграций, ничего не выполняет.
SELECT 1;
