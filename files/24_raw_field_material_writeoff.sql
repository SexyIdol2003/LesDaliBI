-- 24_raw_field_material_writeoff.sql
-- RAW-слой для Document_ДвижениеПродукцииИМатериалов с АпкВидДокумента=
-- 'АктНаСписанияСемянУдобренийИЯдов' и AccumulationRegister_АпкМатериалыВРастениеводствеПоФакту.
-- Закрывает количество и норму расхода удобрений/СЗР на гектар. Рубли будут
-- добавлены после нахождения регистра себестоимости материалов растениеводства.

BEGIN;

CREATE TABLE IF NOT EXISTS raw.r1c_field_material_writeoff_doc (
    _id uuid PRIMARY KEY,
    _deletionmark boolean,
    _posted boolean,
    doc_number text,
    doc_date timestamptz,
    apk_vid_dokumenta text,
    hozyaystvennaya_operaciya text,
    organizaciya_id uuid,
    otpravitel_sklad_id uuid,
    poluchatel_sklad_id uuid,
    apk_vid_raboty_id uuid,
    apk_obekt_zatrat_id uuid,
    _loaded_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_field_material_writeoff_lines (
    doc_id uuid NOT NULL REFERENCES raw.r1c_field_material_writeoff_doc(_id),
    line_number int NOT NULL,
    nomenklatura_id uuid,
    harakteristika_id uuid,
    seriya_id uuid,
    kolichestvo numeric(14,3),
    kolichestvo_upakovok numeric(12,3),
    apk_ploshchad_obrabotannaya numeric(12,3),
    apk_raskhod_na_ga numeric(12,4),
    gruppa_produkcii_id uuid,
    cena numeric(14,2),
    summa numeric(14,2),
    _loaded_at timestamptz DEFAULT now(),
    PRIMARY KEY (doc_id, line_number)
);

CREATE TABLE IF NOT EXISTS raw.r1c_field_material_fact_reg (
    period date,
    organizaciya_id uuid,
    vid_raboty_id uuid,
    nomenklatura_id uuid,
    obekt_zatrat_id uuid,
    sklad_id uuid,
    raskhod_na_ga numeric(12,4),
    kolichestvo numeric(14,3),
    ploshchad_obrabotannaya numeric(12,3),
    recorder_doc_id uuid,
    _loaded_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_field_mat_wo_doc_type
    ON raw.r1c_field_material_writeoff_doc (apk_vid_dokumenta, doc_date);
CREATE INDEX IF NOT EXISTS ix_field_mat_fact_reg_period
    ON raw.r1c_field_material_fact_reg (period, obekt_zatrat_id);

COMMIT;

CREATE OR REPLACE VIEW staging.v_field_agro_input_writeoff AS
SELECT
    d._id AS doc_id,
    d.doc_number,
    d.doc_date::date AS writeoff_date,
    EXTRACT(YEAR FROM d.doc_date)::int AS season_year,
    d.apk_obekt_zatrat_id AS field_id,
    d.apk_vid_raboty_id AS operation_id,
    l.nomenklatura_id,
    l.gruppa_produkcii_id,
    l.kolichestvo,
    l.apk_ploshchad_obrabotannaya AS area_ha,
    l.apk_raskhod_na_ga AS rate_per_ha
FROM raw.r1c_field_material_writeoff_doc d
JOIN raw.r1c_field_material_writeoff_lines l ON l.doc_id = d._id
WHERE d.apk_vid_dokumenta = 'АктНаСписанияСемянУдобренийИЯдов'
  AND COALESCE(d._deletionmark, false) = false
  AND COALESCE(d._posted, false) = true;

CREATE TABLE IF NOT EXISTS mart.fact_agro_input_usage (
    agro_input_sk bigserial PRIMARY KEY,
    season_year int NOT NULL,
    field_sk int REFERENCES mart.dim_field(field_sk),
    nomenklatura_id uuid,
    gruppa_produkcii_id uuid,
    quantity numeric(14,3),
    area_ha numeric(12,3),
    rate_per_ha numeric(12,4),
    cost_rub_per_ha numeric(14,2),
    src_doc_ref text,
    _loaded_at timestamptz DEFAULT now()
);

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
LEFT JOIN mart.dim_field f ON f.ref_key_1c::text = w.field_id::text
WHERE NOT EXISTS (
    SELECT 1 FROM mart.fact_agro_input_usage m WHERE m.src_doc_ref = w.doc_id::text
);
