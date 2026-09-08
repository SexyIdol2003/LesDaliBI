-- 22_raw_fuel_writeoff_summary.sql
-- RAW-слой для Document_АпкСписаниеТопливаПоСуммарнойЗаправке — фактический расход ГСМ
-- по технике за период (месяц). Подтверждено прямым запросом OData 2026-09-08:
--   ФактическийРасход = Заправлено + НачальныйОстаток - КонечныйОстаток
-- Источник: SESSION_2026-09-08_ODATA_DISCOVERY_SUMMARY.md, раздел 6.

BEGIN;

CREATE TABLE IF NOT EXISTS raw.r1c_fuel_writeoff_summary (
    _id uuid PRIMARY KEY,
    _deletionmark boolean,
    _posted boolean,
    doc_number text,
    doc_date timestamptz,
    period_start date,
    period_end date,
    organizaciya_id uuid,
    podrazdelenie_id uuid,
    _loaded_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_fuel_writeoff_summary_gsm (
    doc_id uuid NOT NULL REFERENCES raw.r1c_fuel_writeoff_summary(_id),
    line_number int NOT NULL,
    tehnika_id uuid,
    marka_topliva_id uuid,
    nachalny_ostatok numeric(12,3),
    konechny_ostatok numeric(12,3),
    zapravleno numeric(12,3),
    fakticheskiy_raskhod numeric(12,3),
    _loaded_at timestamptz DEFAULT now(),
    PRIMARY KEY (doc_id, line_number)
);

CREATE INDEX IF NOT EXISTS ix_fuel_wo_summary_tehnika
    ON raw.r1c_fuel_writeoff_summary_gsm (tehnika_id);
CREATE INDEX IF NOT EXISTS ix_fuel_wo_summary_period
    ON raw.r1c_fuel_writeoff_summary (period_start, period_end);

COMMIT;

CREATE OR REPLACE VIEW staging.v_fuel_writeoff_clean AS
SELECT
    g.doc_id,
    g.line_number,
    v.doc_number,
    v.period_start,
    v.period_end,
    DATE_TRUNC('month', v.period_start)::date AS period_month,
    g.tehnika_id,
    g.marka_topliva_id,
    g.nachalny_ostatok,
    g.konechny_ostatok,
    g.zapravleno,
    g.fakticheskiy_raskhod
FROM raw.r1c_fuel_writeoff_summary_gsm g
JOIN raw.r1c_fuel_writeoff_summary v ON v._id = g.doc_id
WHERE COALESCE(v._deletionmark, false) = false
  AND COALESCE(v._posted, false) = true;

CREATE TABLE IF NOT EXISTS mart.fact_fuel_writeoff (
    fuel_writeoff_sk bigserial PRIMARY KEY,
    period_month date NOT NULL,
    equipment_sk int REFERENCES mart.dim_equipment(eq_sk),
    fuel_brand_id uuid,
    liters_start numeric(12,3),
    liters_end numeric(12,3),
    liters_refueled numeric(12,3),
    liters_consumed numeric(12,3),
    src_doc_ref text,
    _loaded_at timestamptz DEFAULT now()
);

INSERT INTO mart.fact_fuel_writeoff (
    period_month, equipment_sk, fuel_brand_id,
    liters_start, liters_end, liters_refueled, liters_consumed, src_doc_ref
)
SELECT
    s.period_month,
    e.eq_sk,
    s.marka_topliva_id,
    s.nachalny_ostatok,
    s.konechny_ostatok,
    s.zapravleno,
    s.fakticheskiy_raskhod,
    s.doc_id::text || '-' || s.line_number::text
FROM staging.v_fuel_writeoff_clean s
LEFT JOIN mart.dim_equipment e ON e.code_1c::text = s.tehnika_id::text
WHERE NOT EXISTS (
    SELECT 1 FROM mart.fact_fuel_writeoff f
    WHERE f.src_doc_ref = s.doc_id::text || '-' || s.line_number::text
);
