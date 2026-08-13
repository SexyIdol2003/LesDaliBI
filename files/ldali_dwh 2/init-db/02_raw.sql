-- ============================================================================
-- RAW-слой: «как из источника»
-- Структура повторяет API/таблицу-источник, всё текстом, плюс служебные поля
-- загрузки (_loaded_at, _source_id) для идемпотентности и инкрементальности.
-- ============================================================================

-- ---- 1С OData справочники ----------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.r1c_fields (
    ref_key         text PRIMARY KEY,           -- 1С Ref_Key (GUID)
    code            text,                       -- АК-00000031 / 00-00000044
    description     text,                       -- наименование (номер поля)
    organization    text,
    department      text,
    is_marked       boolean,                    -- ПометкаУдаления
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    _payload        jsonb                       -- полный ответ OData на случай новых полей
);
COMMENT ON TABLE raw.r1c_fields IS 'Справочник полей из 1С (Справочник.Поля).';

CREATE TABLE IF NOT EXISTS raw.r1c_nomenclature (
    ref_key         text PRIMARY KEY,
    code            text,
    description     text,
    article         text,                       -- артикул 1С (К_ГПР_*, К_ССБ_* и т.п.)
    nomenclature_kind text,                     -- ВидНоменклатуры
    parent_ref      text,                       -- родительская группа
    is_marked       boolean,
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    _payload        jsonb
);
CREATE INDEX IF NOT EXISTS ix_r1c_nom_article ON raw.r1c_nomenclature (article);
CREATE INDEX IF NOT EXISTS ix_r1c_nom_kind    ON raw.r1c_nomenclature (nomenclature_kind);

CREATE TABLE IF NOT EXISTS raw.r1c_equipment (
    ref_key         text PRIMARY KEY,
    code            text,
    description     text,
    is_marked       boolean,
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    _payload        jsonb
);

CREATE TABLE IF NOT EXISTS raw.r1c_warehouses (
    ref_key         text PRIMARY KEY,
    code            text,
    description     text,
    warehouse_type  text,
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    _payload        jsonb
);

CREATE TABLE IF NOT EXISTS raw.r1c_uom (                        -- единицы измерения
    ref_key         text PRIMARY KEY,
    code            text,
    description     text,
    okei            text,
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    _payload        jsonb
);

-- ---- 1С документы: полеводство ---------------------------------------------

-- Урожай с поля (сводный документ выпуска) — заголовок
CREATE TABLE IF NOT EXISTS raw.r1c_harvest_doc (
    doc_ref         text PRIMARY KEY,
    doc_number      text,
    doc_date        date,
    field_ref       text,
    organization    text,
    posted          boolean,
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    _payload        jsonb
);
CREATE INDEX IF NOT EXISTS ix_r1c_harvest_doc_date  ON raw.r1c_harvest_doc (doc_date);
CREATE INDEX IF NOT EXISTS ix_r1c_harvest_doc_field ON raw.r1c_harvest_doc (field_ref);

-- Табличная часть «Продукция»
CREATE TABLE IF NOT EXISTS raw.r1c_harvest_lines (
    doc_ref         text NOT NULL,
    line_no         int  NOT NULL,
    nomenclature_ref text,
    quantity        numeric(18,3),
    uom_ref         text,
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    _payload        jsonb,
    PRIMARY KEY (doc_ref, line_no)
);

-- Списания ТМЦ на поля (СЗР, удобрения, семена)
CREATE TABLE IF NOT EXISTS raw.r1c_field_writeoff_doc (
    doc_ref         text PRIMARY KEY,
    doc_number      text,
    doc_date        date,
    field_ref       text,
    department      text,
    posted          boolean,
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    _payload        jsonb
);
CREATE INDEX IF NOT EXISTS ix_r1c_writeoff_date ON raw.r1c_field_writeoff_doc (doc_date);

CREATE TABLE IF NOT EXISTS raw.r1c_field_writeoff_lines (
    doc_ref         text NOT NULL,
    line_no         int  NOT NULL,
    nomenclature_ref text,
    quantity        numeric(18,3),
    uom_ref         text,
    amount_rub      numeric(18,2),
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    _payload        jsonb,
    PRIMARY KEY (doc_ref, line_no)
);

-- Путевые листы / наряды (для затрат поля и ГСМ)
CREATE TABLE IF NOT EXISTS raw.r1c_waybill (
    doc_ref         text PRIMARY KEY,
    doc_number      text,
    doc_date        date,
    field_ref       text,
    equipment_ref   text,
    operation       text,            -- вспашка / посадка / опрыскивание / уборка ...
    hours           numeric(10,2),
    fuel_liters     numeric(12,2),
    amount_rub      numeric(18,2),
    posted          boolean,
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    _payload        jsonb
);
CREATE INDEX IF NOT EXISTS ix_r1c_waybill_date  ON raw.r1c_waybill (doc_date);
CREATE INDEX IF NOT EXISTS ix_r1c_waybill_field ON raw.r1c_waybill (field_ref);
CREATE INDEX IF NOT EXISTS ix_r1c_waybill_eq    ON raw.r1c_waybill (equipment_ref);

-- ---- Google Sheets: производство --------------------------------------------

-- Сменные данные ЦГП (Выпуск готовой продукции_ЦЛП_сезон 2025)
CREATE TABLE IF NOT EXISTS raw.gs_production_shift (
    sheet_id        text NOT NULL,
    row_idx         int  NOT NULL,
    shift_date      date,
    master          text,
    raw_residue_kg  numeric(18,2),
    plan_kg         numeric(18,2),
    fact_kg         numeric(18,2),
    fact_red_kg     numeric(18,2),
    fact_white_kg   numeric(18,2),
    plan_hours      numeric(10,2),
    fact_hours      numeric(10,2),
    downtime_hours  numeric(10,2),
    secondary_kg    numeric(18,2),
    small_kg        numeric(18,2),
    rotten_kg       numeric(18,2),
    green_kg        numeric(18,2),
    stones_kg       numeric(18,2),
    soil_kg         numeric(18,2),
    other_waste_kg  numeric(18,2),
    productivity_kg_h numeric(12,2),
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    _payload        jsonb,
    PRIMARY KEY (sheet_id, row_idx)
);
COMMENT ON TABLE raw.gs_production_shift IS 'Сменные данные производства из Google Sheets (ЦЛП/ЦГП).';

-- Простои ЦЛП (Аналитика ЦЛП)
CREATE TABLE IF NOT EXISTS raw.gs_downtime (
    sheet_id        text NOT NULL,
    row_idx         int  NOT NULL,
    shift_date      date,
    master          text,
    reason          text,
    duration_h      numeric(10,2),
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    _payload        jsonb,
    PRIMARY KEY (sheet_id, row_idx)
);

-- ---- Журнал загрузок --------------------------------------------------------

CREATE TABLE IF NOT EXISTS meta.load_log (
    id              bigserial PRIMARY KEY,
    source          text NOT NULL,         -- 1c_odata / gsheets / yadisk
    entity          text NOT NULL,         -- r1c_harvest_doc, gs_production_shift, ...
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    rows_inserted   int,
    rows_updated    int,
    rows_failed     int,
    status          text,                  -- success / failed / partial
    error_text      text
);
CREATE INDEX IF NOT EXISTS ix_load_log_entity ON meta.load_log (entity, started_at DESC);
