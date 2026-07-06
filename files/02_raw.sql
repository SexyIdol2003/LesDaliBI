-- =============================================================
-- 02_raw.sql  —  RAW-слой «Лесные дали» DWH
-- Обновлено: 2026-07-06
-- Источники: 1С OData (Catalog_*, Document_*)
-- Объекты реестра:
--   1. Catalog_УпаковкиЕдиницыИзмерения  → raw.r1c_upakovki
--   2. Catalog_АпкМоделиСХТехники        → raw.r1c_sh_tehnika
--   3. Catalog_АпкПоля                   → raw.r1c_polya
--   4. Document_ДвижениеПродукцииИМатериалов → raw.r1c_dvizhenie_produkcii
--   5. Document_АпкПутевойЛистТракториста   → raw.r1c_putevoy_list
-- =============================================================

-- -----------------------------------------------------------
-- БЛОК 1. СПРАВОЧНИКИ (старые — оставлены для совместимости)
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

CREATE TABLE IF NOT EXISTS raw.r1c_counterparties (
    _id             TEXT PRIMARY KEY,
    _deletionmark   BOOLEAN,
    description     TEXT,
    inn             TEXT,
    kpp             TEXT,
    counterparty_kind TEXT,
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

-- -----------------------------------------------------------
-- БЛОК 2. СПРАВОЧНИКИ — РЕЕСТР ОБЪЕКТОВ 1С (5 ключевых)
-- -----------------------------------------------------------

-- 1. Catalog_УпаковкиЕдиницыИзмерения
--    Единицы измерения материалов и продукции (кг, т, л, шт…)
CREATE TABLE IF NOT EXISTS raw.r1c_upakovki (
    _id             TEXT PRIMARY KEY,          -- Ref_Key (GUID)
    _deletionmark   BOOLEAN,
    description     TEXT,                      -- Description (наименование ед.изм.)
    code            TEXT,                      -- Code (ОКЕИ-код или произвольный)
    weight_kg       NUMERIC(12,4),             -- Вес в кг (если задан)
    volume_l        NUMERIC(12,4),             -- Объём в л (если задан)
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

-- 2. Catalog_АпкМоделиСХТехники
--    Справочник моделей сельхозтехники (МТЗ-82, КамАЗ-5320 и пр.)
CREATE TABLE IF NOT EXISTS raw.r1c_sh_tehnika (
    _id                 TEXT PRIMARY KEY,      -- Ref_Key
    _deletionmark       BOOLEAN,
    description         TEXT,                  -- Название модели
    manufacturer        TEXT,                  -- Производитель
    equipment_class     TEXT,                  -- Класс техники (трактор, комбайн, а/м…)
    power_hp            NUMERIC(8,2),           -- Мощность л.с.
    _loaded_at          TIMESTAMPTZ DEFAULT now()
);

-- 3. Catalog_АпкПоля
--    Поля / участки (иерархический справочник)
CREATE TABLE IF NOT EXISTS raw.r1c_polya (
    _id             TEXT PRIMARY KEY,          -- Ref_Key
    _deletionmark   BOOLEAN,
    description     TEXT,                      -- Название поля
    parent_id       TEXT,                      -- Parent_Key (группа/бригада)
    area_ha         NUMERIC(12,4),             -- Площадь, га
    cadastr_num     TEXT,                      -- Кадастровый номер
    farm_id         TEXT,                      -- Хозяйство (ссылка)
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

-- -----------------------------------------------------------
-- БЛОК 3. ДОКУМЕНТЫ — РЕЕСТР ОБЪЕКТОВ 1С
-- -----------------------------------------------------------

-- 4. Document_ДвижениеПродукцииИМатериалов
--    Ключевой документ: списание материалов + оприходование урожая.
--    Разбивается по полю «Операция» на два факта в mart-слое.
CREATE TABLE IF NOT EXISTS raw.r1c_dvizhenie_produkcii (
    _id                 TEXT PRIMARY KEY,      -- Ref_Key документа
    _deletionmark       BOOLEAN,
    _posted             BOOLEAN,
    doc_number          TEXT,                  -- Number
    doc_date            TIMESTAMPTZ,           -- Date
    operaciya           TEXT,                  -- Операция: «СписаниеМатериалов» | «ОприходованиеУрожая» | …
    sklad_id            TEXT,                  -- Склад (Ref_Key → raw.r1c_warehouses)
    kontragent_id       TEXT,                  -- Контрагент (опционально)
    kommentariy         TEXT,
    _loaded_at          TIMESTAMPTZ DEFAULT now()
);

-- Табличная часть: строки документа ДвижениеПродукцииИМатериалов
CREATE TABLE IF NOT EXISTS raw.r1c_dvizhenie_produkcii_lines (
    _id                 TEXT PRIMARY KEY,      -- составной ключ: doc_id + LineNumber
    doc_id              TEXT NOT NULL REFERENCES raw.r1c_dvizhenie_produkcii(_id),
    line_number         INT,
    pole_id             TEXT,                  -- Поле → raw.r1c_polya
    nomenklatura_id     TEXT,                  -- Номенклатура → raw.r1c_nomenclature
    edinica_id          TEXT,                  -- Ед.изм. → raw.r1c_upakovki
    kolichestvo         NUMERIC(18,3),
    summa               NUMERIC(18,2),
    seriya              TEXT,                  -- Серия (партия)
    _loaded_at          TIMESTAMPTZ DEFAULT now()
);

-- 5. Document_АпкПутевойЛистТракториста
--    Путевой лист: выработка техники, расход топлива, операции на полях.
CREATE TABLE IF NOT EXISTS raw.r1c_putevoy_list (
    _id                 TEXT PRIMARY KEY,
    _deletionmark       BOOLEAN,
    _posted             BOOLEAN,
    doc_number          TEXT,
    doc_date            TIMESTAMPTZ,
    data_nachala        TIMESTAMPTZ,           -- Дата начала смены
    data_okonchaniya    TIMESTAMPTZ,           -- Дата окончания смены
    tehnika_id          TEXT,                  -- Техника (объект, не модель)
    model_tehniki_id    TEXT,                  -- Модель техники → raw.r1c_sh_tehnika
    voditel_id          TEXT,                  -- Водитель/тракторист → raw.r1c_employees
    narabotka_moto_chas NUMERIC(10,2),         -- Моточасы
    probeg_km           NUMERIC(10,2),
    toplivo_vydano      NUMERIC(12,3),
    toplivo_vozvrat     NUMERIC(12,3),
    kommentariy         TEXT,
    _loaded_at          TIMESTAMPTZ DEFAULT now()
);

-- Табличная часть путевого листа: операции на полях
CREATE TABLE IF NOT EXISTS raw.r1c_putevoy_list_lines (
    _id                 TEXT PRIMARY KEY,
    doc_id              TEXT NOT NULL REFERENCES raw.r1c_putevoy_list(_id),
    line_number         INT,
    pole_id             TEXT,                  -- Поле → raw.r1c_polya
    agr_operaciya_id    TEXT,                  -- Агрооперация
    edinica_id          TEXT,                  -- Ед.изм. → raw.r1c_upakovki
    obem_rabot_ga       NUMERIC(12,4),         -- Объём работ, га
    norma_vyrabotki     NUMERIC(10,3),
    _loaded_at          TIMESTAMPTZ DEFAULT now()
);

-- -----------------------------------------------------------
-- БЛОК 4. GOOGLE SHEETS (без изменений)
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
