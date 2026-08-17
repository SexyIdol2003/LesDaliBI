-- 06_new_raw_sources.sql
-- Новые raw-источники и парсинг связи "поле" по итогам расследования SESSION_2026-08-17
-- ВНИМАНИЕ: имена OData-сущностей ниже — предположение по конвенции проекта (Document_<Имя1С>),
-- т.к. $metadata возвращает HTTP 500 (см. SESSION_2026-08-15_BI_1C_AUDIT_AND_ODATA.md).
-- Перед запуском DAG нужно сверить их с реальным ответом OData (см. task `verify_entity` в каждом новом DAG).

-- ============================================================
-- 1. Регистрация взвешиваний на току — первичный источник урожая
-- ============================================================
CREATE TABLE IF NOT EXISTS raw.r1c_tok_vzveshivanie (
    _id                 uuid PRIMARY KEY,
    _deletionmark       boolean,
    _posted             boolean,
    doc_number          text,
    doc_date            timestamp,
    vid_vzveshivaniya   text,      -- 'Урожай с поля' и др.
    otkuda_text         text,      -- закодированный склад/подразделение: "318, КаГалаРС2ПУч.6 2025 г. (...)"
    kuda_text           text,      -- обычно 'Бункер'
    nomenklatura_text   text,      -- "Урожай 2025 ГалаРС3 Белый с поливом., поле 318, уч.6"
    voditel_id          uuid,
    avtomobil_id        uuid,
    ves_tary_kg         numeric(18,3),
    ves_brutto_kg       numeric(18,3),
    ves_netto_kg        numeric(18,3),
    vesovshik_id        uuid,
    _loaded_at          timestamp DEFAULT now()
);

-- ============================================================
-- 2. Акты на списание семян, удобрений и ядов — материальные затраты по полю
--    (расширяем уже существующие raw.r1c_field_writeoff_doc/lines новыми колонками,
--     не пересоздавая таблицы, т.к. они уже описаны в 02_raw.sql)
-- ============================================================
ALTER TABLE raw.r1c_field_writeoff_doc ADD COLUMN IF NOT EXISTS otpravitel_text text;   -- 'Поле 318' (чистый текст)
ALTER TABLE raw.r1c_field_writeoff_doc ADD COLUMN IF NOT EXISTS poluchatel_text text;   -- закодированная партия
ALTER TABLE raw.r1c_field_writeoff_doc ADD COLUMN IF NOT EXISTS operaciya text;
ALTER TABLE raw.r1c_field_writeoff_doc ADD COLUMN IF NOT EXISTS otvetstvennyi_id uuid;

ALTER TABLE raw.r1c_field_writeoff_lines ADD COLUMN IF NOT EXISTS nomenklatura_id uuid;
ALTER TABLE raw.r1c_field_writeoff_lines ADD COLUMN IF NOT EXISTS kolichestvo numeric(18,3);
ALTER TABLE raw.r1c_field_writeoff_lines ADD COLUMN IF NOT EXISTS edinica_id uuid;

-- ============================================================
-- 3. Списание топлива по суммарной заправке — расход ГСМ по технике за месяц
-- ============================================================
CREATE TABLE IF NOT EXISTS raw.r1c_fuel_summary_writeoff (
    _id                 uuid PRIMARY KEY,
    _deletionmark       boolean,
    doc_number          text,
    doc_date            timestamp,
    period_start        date,
    period_end          date,
    organization_id     uuid,
    department_id       uuid,
    _loaded_at          timestamp DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_fuel_summary_writeoff_lines (
    doc_id              uuid REFERENCES raw.r1c_fuel_summary_writeoff(_id),
    line_number         int,
    equipment_id        uuid,
    fuel_brand          text,
    nachalny_ostatok_l  numeric(18,3),
    zapravleno_l        numeric(18,3),
    fakticheskiy_rashod_l numeric(18,3),
    konechny_ostatok_l  numeric(18,3),
    _loaded_at          timestamp DEFAULT now(),
    PRIMARY KEY (doc_id, line_number)
);

-- ============================================================
-- 4. Этапы производства — статусы по полям/партиям, расшифровка культур
-- ============================================================
CREATE TABLE IF NOT EXISTS raw.r1c_production_stages (
    _id                 uuid PRIMARY KEY,
    doc_number          text,
    doc_date            timestamp,
    organization_id     uuid,
    podrazdelenie_text  text,      -- '318, КаГалаРС1ПУч.9_318 2026 г.' / '318, Пар 2026 г.' / '868, Заросли 2026 г.'
    status              text,      -- 'Начат' и др.
    otvetstvennyi_id    uuid,
    _loaded_at          timestamp DEFAULT now()
);

-- ============================================================
-- 5. Универсальный парсинг номера поля из закодированного текста
-- ============================================================
CREATE SCHEMA IF NOT EXISTS staging;

-- Вариант А: текст вида "318, КаГалаРС2ПУч.6 2025 г. (...)" — номер до первой запятой
CREATE OR REPLACE FUNCTION staging.extract_field_number(input_text text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT NULLIF(TRIM(SPLIT_PART(input_text, ',', 1)), '')
$$;

-- Вариант Б: текст вида "Поле 318" — номер после префикса "Поле "
CREATE OR REPLACE FUNCTION staging.extract_field_number_prefixed(input_text text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT NULLIF(TRIM(REGEXP_REPLACE(input_text, '^Поле\s+', '', 'i')), '')
$$;

-- Единая функция: пробует оба варианта, чтобы использовать её везде одинаково
CREATE OR REPLACE FUNCTION staging.extract_field_number_any(input_text text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT COALESCE(
        staging.extract_field_number_prefixed(input_text),
        staging.extract_field_number(input_text)
    )
$$;

-- ============================================================
-- 6. Staging: очищенные взвешивания на току со связью на поле
-- ============================================================
CREATE OR REPLACE VIEW staging.v_tok_vzveshivanie_clean AS
SELECT
    t._id,
    t.doc_number,
    t.doc_date,
    t.vid_vzveshivaniya,
    t.otkuda_text,
    t.kuda_text,
    t.nomenklatura_text,
    t.ves_netto_kg,
    t.ves_brutto_kg,
    t.ves_tary_kg,
    staging.extract_field_number_any(t.otkuda_text) AS pole_nomer,
    p._id AS pole_id,
    EXTRACT(YEAR FROM t.doc_date)::int AS god_urozhaya
FROM raw.r1c_tok_vzveshivanie t
LEFT JOIN raw.r1c_polya p
    ON p.naimenovanie = staging.extract_field_number_any(t.otkuda_text)
WHERE COALESCE(t._deletionmark, false) = false;

-- ============================================================
-- 7. Mart: урожай по полю на основе взвешиваний на току (замена агрегатов из кладовой)
-- ============================================================
CREATE OR REPLACE VIEW mart.v_fact_harvest_tok AS
SELECT
    v.pole_id,
    v.pole_nomer,
    v.god_urozhaya,
    v.doc_date,
    v.nomenklatura_text,
    v.ves_netto_kg AS kolichestvo_kg,
    CASE WHEN v.pole_id IS NULL THEN false ELSE true END AS is_pole_resolved
FROM staging.v_tok_vzveshivanie_clean v
WHERE v.vid_vzveshivaniya = 'Урожай с поля';

-- ============================================================
-- 8. Staging: очищенные акты списания семян/удобрений/ядов со связью на поле
-- ============================================================
CREATE OR REPLACE VIEW staging.v_field_writeoff_clean AS
SELECT
    d._id,
    d.doc_number,
    d.doc_date,
    d.otpravitel_text,
    d.poluchatel_text,
    d.operaciya,
    staging.extract_field_number_any(d.otpravitel_text) AS pole_nomer,
    p._id AS pole_id,
    l.nomenklatura_id,
    l.kolichestvo,
    l.edinica_id
FROM raw.r1c_field_writeoff_doc d
JOIN raw.r1c_field_writeoff_lines l ON l.doc_id = d._id
LEFT JOIN raw.r1c_polya p
    ON p.naimenovanie = staging.extract_field_number_any(d.otpravitel_text)
WHERE COALESCE(d._deletionmark, false) = false;
