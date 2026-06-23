-- ============================================================================
-- МИГРАЦИЯ v0.1 → v0.2: исправление сезонной логики и добавление измерений
-- по итогам сверки с «Общим описанием бизнес-процессов компании»
--
-- ПРИМЕНЕНИЕ:
--   docker compose exec -T postgres-dwh psql -U ldali_admin -d ldali_dwh < migrations/2026-05-08_align_with_bp.sql
--
-- Изменения:
--   1. dim_date: граница сезона 1 июля → 1 октября (по «Отчёту о закладке урожая»)
--   2. dim_date: фазы строго по БП (подготовка / посевная / вегетация / уборка)
--   3. dim_date: новое поле sales_phase (продажи_собственное / продажи_покупное)
--   4. mart.dim_customer: новый справочник покупателей и каналов сбыта
--   5. fact_production_output: поля raw_material_source и processing_type
-- ============================================================================

BEGIN;

-- (3) Новое поле sales_phase
ALTER TABLE mart.dim_date
    ADD COLUMN IF NOT EXISTS sales_phase text;

COMMENT ON COLUMN mart.dim_date.season       IS 'Сельхоз-сезон: 1 окт N — 30 сен N+1, маркируется годом N (по БП ООО «Лесные дали»).';
COMMENT ON COLUMN mart.dim_date.agro_phase   IS 'Фаза агро-цикла из БП: подготовка | посевная | вегетация | уборка.';
COMMENT ON COLUMN mart.dim_date.sales_phase  IS 'Фаза продаж из БП: продажи_собственное | продажи_покупное.';

-- (1) и (2): пересчитать season, season_label, agro_phase, sales_phase для всех дат
UPDATE mart.dim_date SET
    season = CASE WHEN month >= 10 THEN year ELSE year - 1 END,
    season_label = CASE
        WHEN month >= 10 THEN year || '-' || (year + 1)
        ELSE (year - 1) || '-' || year
    END,
    agro_phase = CASE
        WHEN month IN (10, 11, 12, 1, 2, 3) THEN 'подготовка'
        WHEN month IN (4, 5)                THEN 'посевная'
        WHEN month IN (6, 7)                THEN 'вегетация'
        WHEN month = 8 AND EXTRACT(DAY FROM date_day)::int < 15  THEN 'вегетация'
        WHEN month = 8 AND EXTRACT(DAY FROM date_day)::int >= 15 THEN 'уборка'
        WHEN month = 9                      THEN 'уборка'
    END,
    sales_phase = CASE
        WHEN month = 4 AND EXTRACT(DAY FROM date_day)::int >= 15 THEN 'продажи_покупное'
        WHEN month IN (5, 6, 7, 8, 9)                            THEN 'продажи_покупное'
        ELSE 'продажи_собственное'
    END;

-- Проверка результата
DO $$
DECLARE
    v_count_dates int;
    v_seasons     text;
    v_phases      text;
BEGIN
    SELECT count(*) INTO v_count_dates FROM mart.dim_date;
    SELECT string_agg(DISTINCT season::text, ', ' ORDER BY season::text) INTO v_seasons
        FROM mart.dim_date WHERE year BETWEEN 2023 AND 2026;
    SELECT string_agg(DISTINCT agro_phase, ', ') INTO v_phases FROM mart.dim_date;
    RAISE NOTICE 'dim_date: % строк', v_count_dates;
    RAISE NOTICE 'Сезоны 2023–2026: %', v_seasons;
    RAISE NOTICE 'Уникальные агро-фазы: %', v_phases;
END$$;

-- (4) Новый dim_customer
CREATE TABLE IF NOT EXISTS mart.dim_customer (
    customer_sk     bigserial PRIMARY KEY,
    code_1c         text UNIQUE,
    name            text NOT NULL,
    sales_channel   text NOT NULL,
    network_name    text,
    is_active       boolean NOT NULL DEFAULT true
);
COMMENT ON TABLE mart.dim_customer IS 'Покупатели и каналы сбыта по БП: сетевые магазины, HoReCa, оптовые покупатели.';

GRANT SELECT ON mart.dim_customer TO datalens_ro;
GRANT ALL    ON mart.dim_customer TO dbt_runner;

-- (5) Поля переработки в fact_production_output
ALTER TABLE mart.fact_production_output
    ADD COLUMN IF NOT EXISTS raw_material_source text,
    ADD COLUMN IF NOT EXISTS processing_type     text;

COMMENT ON COLUMN mart.fact_production_output.raw_material_source IS 'Источник сырья по БП: собственное (1 окт – 15 апр) | покупное (15 апр – 1 окт).';
COMMENT ON COLUMN mart.fact_production_output.processing_type     IS 'Тип переработки по БП: легкая (мойка/чистка/фасовка) | глубокая (вакуум/варка).';

CREATE INDEX IF NOT EXISTS ix_fact_prod_src
    ON mart.fact_production_output (raw_material_source, processing_type);

COMMIT;

-- ============================================================================
-- Контрольные запросы (запустить руками для проверки):
--
--   SELECT season, season_label, count(*) AS days
--   FROM mart.dim_date WHERE year BETWEEN 2024 AND 2025
--   GROUP BY 1,2 ORDER BY 1;
--
--   SELECT agro_phase, sales_phase, count(*) AS days
--   FROM mart.dim_date WHERE season = 2024
--   GROUP BY 1,2 ORDER BY 1,2;
-- ============================================================================
