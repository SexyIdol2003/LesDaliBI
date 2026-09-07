-- 14_raw_zapravochnaya_vedomost.sql
-- RAW-слой для Document_АпкЗаправочныеВедомости — первый подтверждённый факт ГСМ.
-- Подтверждено прямым запросом OData 2026-09-07:
--   GET .../Document_АпкЗаправочныеВедомости?$format=json&$top=1
-- Реальные поля документа и табличной части ГСМ проверены на живых данных.

BEGIN;

CREATE TABLE IF NOT EXISTS raw.r1c_zapravochnaya_vedomost (
    _id                       uuid PRIMARY KEY,          -- Ref_Key
    _deletionmark             boolean,
    _posted                   boolean,
    doc_number                text,                       -- Number
    doc_date                  timestamptz,                -- Date
    organizaciya_id           uuid,                       -- Организация_Key
    podrazdelenie_id          uuid,                       -- Подразделение_Key
    sklad_id                  uuid,                       -- Склад_Key
    tehnika_id                uuid,                       -- ТранспортноеСредство_Key (техника документа)
    avtor_id                  uuid,                       -- Автор_Key
    sotrudnik_id              uuid,                       -- Сотрудник (водитель/ответственный)
    kommentariy               text,
    vydacha_s_toplivozapravschika boolean,                -- ВыдачаСТопливозаправщика
    naemnoe_ts                boolean,                    -- НаемноеТС
    partner_id                uuid,                       -- Партнер_Key (для наёмного ТС)
    soglashenie_id            uuid,                       -- Соглашение_Key
    _loaded_at                timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_zapravochnaya_vedomost_gsm (
    doc_id                    uuid NOT NULL REFERENCES raw.r1c_zapravochnaya_vedomost(_id),
    line_number                int,
    line_date                  date,                       -- Дата (дата конкретной заправки)
    line_time                  time,                       -- Время
    marka_topliva_id            uuid,                       -- МаркаТоплива_Key
    kolichestvo                 numeric(12,3),              -- Количество, л
    line_tehnika_id              uuid,                       -- ТранспортноеСредство_Key в строке (обычно 0, используем заголовок)
    line_sotrudnik_id             uuid,                       -- Сотрудник в строке
    _loaded_at                   timestamptz DEFAULT now(),
    PRIMARY KEY (doc_id, line_number)
);

CREATE INDEX IF NOT EXISTS ix_zapr_ved_tehnika_date
    ON raw.r1c_zapravochnaya_vedomost (tehnika_id, doc_date);

CREATE INDEX IF NOT EXISTS ix_zapr_ved_gsm_date
    ON raw.r1c_zapravochnaya_vedomost_gsm (line_date);

COMMIT;

-- ============================================================
-- Staging: заправки с реальной техникой (заголовок, т.к. в строках техника = 0)
-- ============================================================
CREATE OR REPLACE VIEW staging.v_zapravki_clean AS
SELECT
    g.doc_id,
    g.line_number,
    v.doc_number,
    COALESCE(g.line_date, v.doc_date::date) AS zapravka_date,
    v.tehnika_id,                       -- берём технику из заголовка, не из строки
    g.marka_topliva_id,
    g.kolichestvo AS litry,
    v.podrazdelenie_id,
    v.sklad_id
FROM raw.r1c_zapravochnaya_vedomost_gsm g
JOIN raw.r1c_zapravochnaya_vedomost v ON v._id = g.doc_id
WHERE COALESCE(v._deletionmark, false) = false;

-- ============================================================
-- Mart: факт заправки по технике/дате (пока без разбивки по операции)
-- ============================================================
CREATE TABLE IF NOT EXISTS mart.fact_fuel_refuel (
    fuel_refuel_sk   bigserial PRIMARY KEY,
    date_day          date NOT NULL,
    equipment_sk       int REFERENCES mart.dim_equipment(equipment_sk),
    fuel_brand_id       uuid,
    liters               numeric(12,3),
    src_doc_ref          text,
    _loaded_at            timestamptz DEFAULT now()
);

-- Наполнение mart.fact_fuel_refuel из staging (сопоставление техники по 1С Ref_Key -> dim_equipment)
-- Проверить фактическое имя ключевого поля связи в dim_equipment (ref_key_1c / equipment_code_1c) перед запуском.
INSERT INTO mart.fact_fuel_refuel (date_day, equipment_sk, fuel_brand_id, liters, src_doc_ref)
SELECT
    z.zapravka_date,
    e.equipment_sk,
    z.marka_topliva_id,
    z.litry,
    z.doc_id::text || '-' || z.line_number::text
FROM staging.v_zapravki_clean z
LEFT JOIN mart.dim_equipment e ON e.ref_key_1c::text = z.tehnika_id::text
WHERE NOT EXISTS (
    SELECT 1 FROM mart.fact_fuel_refuel f
    WHERE f.src_doc_ref = z.doc_id::text || '-' || z.line_number::text
);

-- Контроль:
-- SELECT count(*) AS zapravok, round(sum(liters),1) AS total_litrov,
--        min(date_day) AS ot, max(date_day) AS do
-- FROM mart.fact_fuel_refuel;
