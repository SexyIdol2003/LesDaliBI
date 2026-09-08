-- 23_raw_equipment_repair.sql
-- RAW-слой для Document_АпкВыполнениеРемонтаТС — фактические затраты труда/зарплаты
-- по ремонту техники. Материалы и внешние работы будут добавлены после проверки
-- заполненных строк соответствующих табличных частей.

BEGIN;

CREATE TABLE IF NOT EXISTS raw.r1c_equipment_repair (
    _id uuid PRIMARY KEY,
    _deletionmark boolean,
    _posted boolean,
    status text,
    tehnika_id uuid,
    obekt_ekspluatacii_id uuid,
    data_nachala_fact date,
    data_zaversheniya_fact date,
    zakaz_na_remont_id uuid,
    _loaded_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_equipment_repair_details (
    doc_id uuid NOT NULL REFERENCES raw.r1c_equipment_repair(_id),
    line_number int NOT NULL,
    uzel_id uuid,
    vid_remonta_id uuid,
    opisanie_remonta text,
    prichina_remonta text,
    _loaded_at timestamptz DEFAULT now(),
    PRIMARY KEY (doc_id, line_number)
);

CREATE TABLE IF NOT EXISTS raw.r1c_equipment_repair_works (
    doc_id uuid NOT NULL REFERENCES raw.r1c_equipment_repair(_id),
    line_number int NOT NULL,
    vid_raboty_id uuid,
    kolichestvo numeric(12,3),
    rascenka numeric(14,2),
    chasov numeric(10,3),
    chasov_po_norme numeric(10,3),
    osnovnaya_zp numeric(14,2),
    dopolnitelnaya_zp numeric(14,2),
    itogo_zp numeric(14,2),
    ispolnitel_id uuid,
    _loaded_at timestamptz DEFAULT now(),
    PRIMARY KEY (doc_id, line_number)
);

CREATE INDEX IF NOT EXISTS ix_equip_repair_tehnika_date
    ON raw.r1c_equipment_repair (tehnika_id, data_nachala_fact);

COMMIT;

CREATE OR REPLACE VIEW staging.v_equipment_repair_clean AS
SELECT
    r._id AS doc_id,
    r.tehnika_id,
    r.data_nachala_fact,
    r.data_zaversheniya_fact,
    DATE_TRUNC('month', r.data_nachala_fact)::date AS repair_month,
    w.vid_raboty_id,
    w.chasov,
    w.osnovnaya_zp,
    w.dopolnitelnaya_zp,
    w.itogo_zp
FROM raw.r1c_equipment_repair r
JOIN raw.r1c_equipment_repair_works w ON w.doc_id = r._id
WHERE COALESCE(r._deletionmark, false) = false
  AND COALESCE(r._posted, false) = true;

CREATE TABLE IF NOT EXISTS mart.fact_equipment_repair_cost (
    repair_cost_sk bigserial PRIMARY KEY,
    repair_month date NOT NULL,
    equipment_sk int REFERENCES mart.dim_equipment(equipment_sk),
    labor_hours numeric(10,3),
    labor_cost_rub numeric(14,2),
    cost_type text DEFAULT 'labor_only',
    src_doc_ref text,
    _loaded_at timestamptz DEFAULT now()
);

INSERT INTO mart.fact_equipment_repair_cost (
    repair_month, equipment_sk, labor_hours, labor_cost_rub, src_doc_ref
)
SELECT
    s.repair_month,
    e.equipment_sk,
    s.chasov,
    s.itogo_zp,
    s.doc_id::text
FROM staging.v_equipment_repair_clean s
LEFT JOIN mart.dim_equipment e ON e.ref_key_1c::text = s.tehnika_id::text
WHERE NOT EXISTS (
    SELECT 1 FROM mart.fact_equipment_repair_cost f WHERE f.src_doc_ref = s.doc_id::text
);
