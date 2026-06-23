-- ============================================================================
-- ООО «Лесные дали» — DWH, инициализация: схемы и роли
-- Этап 1.1 плана-графика. Запускается postgres-контейнером один раз при init.
-- ============================================================================

-- Слои хранилища
CREATE SCHEMA IF NOT EXISTS raw;        -- сырые данные «как есть» из источников
CREATE SCHEMA IF NOT EXISTS staging;    -- очищенные, типизированные, нормализованные
CREATE SCHEMA IF NOT EXISTS mart;       -- витрины звёздной схемы (для DataLens)
CREATE SCHEMA IF NOT EXISTS meta;       -- служебная: маппинги, журналы загрузок

COMMENT ON SCHEMA raw     IS 'Сырые данные источников (1С OData, Google Sheets, Яндекс.Диск). Без обработки.';
COMMENT ON SCHEMA staging IS 'Типизированные и очищенные данные. Парсинг 1С-артикулов, дедуп.';
COMMENT ON SCHEMA mart    IS 'Звёздная схема витрин: dim_* и fact_*. Источник для DataLens и ML.';
COMMENT ON SCHEMA meta    IS 'Служебные таблицы: маппинги ключей, журналы загрузок, словари.';

-- Read-only роль для DataLens / аналитиков / отчётности
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'datalens_ro') THEN
    CREATE ROLE datalens_ro LOGIN PASSWORD 'datalens_ro_change_me';
  END IF;
END$$;

GRANT USAGE ON SCHEMA mart TO datalens_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA mart GRANT SELECT ON TABLES TO datalens_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO datalens_ro;

-- Роль для dbt (полный доступ к staging/mart, чтение raw)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dbt_runner') THEN
    CREATE ROLE dbt_runner LOGIN PASSWORD 'dbt_runner_change_me';
  END IF;
END$$;

GRANT USAGE ON SCHEMA raw TO dbt_runner;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO dbt_runner;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT SELECT ON TABLES TO dbt_runner;

GRANT ALL ON SCHEMA staging, mart TO dbt_runner;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO dbt_runner;
ALTER DEFAULT PRIVILEGES IN SCHEMA mart GRANT ALL ON TABLES TO dbt_runner;
