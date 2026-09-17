-- ============================================================================
-- 32_fix_agro_input_usage_via_struktura.sql
-- Финальное решение для показателя №3 (ТМЦ и материалы: семена/удобрения/СЗР).
--
-- Проблема: mart.fact_agro_input_usage была 0 строк, потому что JOIN в
-- 24_raw_field_material_writeoff.sql ссылался на несуществующую колонку
-- mart.dim_field.ref_key_1c.
--
-- Расследование (сессия 2026-09-17) показало реальную цепочку
-- связи:
--   raw.r1c_field_material_writeoff_doc.apk_obekt_zatrat_id (АпкОбъектЗатрат_Key)
--     -> Catalog_СтруктураПредприятия._id  (элемент затрат, напр. "201, Сидераты...")
--       -> АпкПоле_Key
--         -> Catalog_АпкПоля._id = mart.dim_field.field_code_1c (GUID поля)
--
-- Catalog_СтруктураПредприятия никогда не выгружался в raw. Выгружен через
-- curl (1184 строки, 2 страницы по $top=1000/$skip) и загружен ниже.
--
-- Результат проверки: 176 из 188 объектов затрат нашли соответствие в
-- Catalog_СтруктураПредприятия; 1730 из 1854 документов (93%) получили
-- привязку к полю; итоговая загрузка mart.fact_agro_input_usage — 4507 из
-- 4861 строки raw (92.7%). Остальные строки — легитимные списания на
-- цеховые склады без привязки к конкретному полю.
-- ============================================================================

-- ---- 1. RAW-таблица для Catalog_СтруктураПредприятия -----------------------

CREATE TABLE IF NOT EXISTS raw.r1c_struktura_predpriyatiya (
    ref_key uuid PRIMARY KEY,
    code text,
    description text,
    apk_pole_key uuid,
    deletion_mark boolean,
    _loaded_at timestamptz DEFAULT now()
);

-- Загрузка (выполняется вручную, см. SESSION_2026-09-17_MASTER_STATE_AND_METRICS_AUDIT.md
-- раздел "Обновление 17.09.2026" за точный скрипт curl/python):
--
--   \copy raw.r1c_struktura_predpriyatiya_stage FROM '/tmp/struktura.csv'
--       WITH (FORMAT csv, HEADER true, NULL '')
--
--   INSERT INTO raw.r1c_struktura_predpriyatiya (...)
--   SELECT DISTINCT ... FROM raw.r1c_struktura_predpriyatiya_stage
--   ON CONFLICT (ref_key) DO NOTHING;
--
-- (staging-таблица нужна, т.к. 1С OData не гарантирует стабильную сортировку
-- при пагинации $top/$skip без $orderby, из-за чего в выгрузке встречаются
-- дубли ref_key).

-- ---- 2. Финальная загрузка mart.fact_agro_input_usage ----------------------

TRUNCATE mart.fact_agro_input_usage;

INSERT INTO mart.fact_agro_input_usage (
    season_year, field_sk, nomenklatura_id, gruppa_produkcii_id,
    quantity, area_ha, rate_per_ha, src_doc_ref
)
SELECT
    w.season_year,
    f.field_sk,
    w.nomenklatura_id,
    w.gruppa_produkcii_id,
    w.kolichestvo,
    w.area_ha,
    w.rate_per_ha,
    w.doc_id::text
FROM staging.v_field_agro_input_writeoff w
JOIN raw.r1c_struktura_predpriyatiya s ON s.ref_key = w.field_id
JOIN mart.dim_field f ON f.field_code_1c = s.apk_pole_key::text AND f.is_current
WHERE NOT EXISTS (
    SELECT 1 FROM mart.fact_agro_input_usage m WHERE m.src_doc_ref = w.doc_id::text
);

-- Контроль после применения:
-- SELECT count(*) FROM mart.fact_agro_input_usage;  -- ожидается 4507
-- SELECT f.field_name, count(*), round(sum(m.quantity),2)
-- FROM mart.fact_agro_input_usage m
-- JOIN mart.dim_field f ON f.field_sk = m.field_sk
-- GROUP BY f.field_name ORDER BY 2 DESC LIMIT 10;
