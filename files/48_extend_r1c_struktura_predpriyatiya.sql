-- 48_extend_r1c_struktura_predpriyatiya.sql
-- Расширение справочника Catalog_СтруктураПредприятия для моста
-- аналитика расходов -> сезонный объект -> поле.

ALTER TABLE raw.r1c_struktura_predpriyatiya
    ADD COLUMN IF NOT EXISTS apk_harvest_year integer,
    ADD COLUMN IF NOT EXISTS status text;

COMMENT ON COLUMN raw.r1c_struktura_predpriyatiya.apk_harvest_year IS
    'АпкГодУрожая из Catalog_СтруктураПредприятия (1С OData).';

COMMENT ON COLUMN raw.r1c_struktura_predpriyatiya.status IS
    'Статус элемента структуры предприятия из 1С OData.';
