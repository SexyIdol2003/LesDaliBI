-- =============================================================
-- 02_raw.sql  —  RAW-слой «Лесные дали» DWH
-- Обновлено: 2026-06-23
-- Источники: 1С OData (Catalog_*), Google Sheets
-- =============================================================

-- -----------------------------------------------------------
-- 1. СПРАВОЧНИКИ (уже были)
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.r1c_nomenclature (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    parent_id       TEXT,
    article         TEXT,
    base_unit_id    TEXT,
    nomen_group_id  TEXT,
    nomen_kind      TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_warehouses (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    parent_id       TEXT,
    warehouse_type  TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_uom (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    code            TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_equipment (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    model_id        TEXT,
    reg_number      TEXT,
    equipment_type  TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_fields (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    parent_id       TEXT,
    area_ha         NUMERIC(12,4),
    farm_id         TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

-- -----------------------------------------------------------
-- 2. СПРАВОЧНИКИ (новые — добавлены 2026-06-23)
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.r1c_crops (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    parent_id       TEXT,
    crop_kind       TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_counterparties (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    inn             TEXT,
    kpp             TEXT,
    counterparty_kind TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_company_structure (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    parent_id       TEXT,
    inn             TEXT,
    node_type       TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_expense_items (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    parent_id       TEXT,
    expense_kind    TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_cashflow_items (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    parent_id       TEXT,
    cashflow_kind   TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_tech_operations (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    parent_id       TEXT,
    operation_kind  TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_crop_rotations (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_fuel_cards (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    card_number     TEXT,
    owner_id        TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_equipment_models (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    manufacturer    TEXT,
    equipment_class TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_farms (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    parent_id       TEXT,
    address         TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_nomen_characteristics (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    nomenclature_id TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_employees (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    individual_id   TEXT,
    position_id     TEXT,
    department_id   TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_warehouse_rooms (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    warehouse_id    TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

-- -----------------------------------------------------------
-- 3. ДОКУМЕНТЫ (уже были)
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.r1c_harvest_doc (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    _posted         BOOLEAN,
    doc_number      TEXT,
    doc_date        DATE,
    field_id        TEXT,
    crop_id         TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_harvest_lines (
    _id             TEXT PRIMARY KEY,
    doc_id          TEXT REFERENCES raw.r1c_harvest_doc(_id),
    line_number     INT,
    nomenclature_id TEXT,
    quantity        NUMERIC(18,3),
    uom_id          TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_field_writeoff_doc (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    _posted         BOOLEAN,
    doc_number      TEXT,
    doc_date        DATE,
    field_id        TEXT,
    expense_item_id TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_field_writeoff_lines (
    _id             TEXT PRIMARY KEY,
    doc_id          TEXT REFERENCES raw.r1c_field_writeoff_doc(_id),
    line_number     INT,
    nomenclature_id TEXT,
    quantity        NUMERIC(18,3),
    amount          NUMERIC(18,2),
    uom_id          TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_waybill (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    _posted         BOOLEAN,
    doc_number      TEXT,
    doc_date        DATE,
    vehicle_id      TEXT,
    driver_id       TEXT,
    fuel_card_id    TEXT,
    fuel_qty        NUMERIC(12,3),
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

-- -----------------------------------------------------------
-- 4. GOOGLE SHEETS (без изменений)
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.gs_production_shift (
    id              SERIAL PRIMARY KEY,
    shift_date      DATE,
    field_id        TEXT,
    crop_id         TEXT,
    equipment_id    TEXT,
    employee_id     TEXT,
    operation       TEXT,
    area_ha         NUMERIC(12,4),
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.gs_downtime (
    id              SERIAL PRIMARY KEY,
    downtime_date   DATE,
    equipment_id    TEXT,
    reason          TEXT,
    duration_hours  NUMERIC(8,2),
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);
