-- 07_fix_field_linkage.sql
-- Исправление ошибок из 06_new_raw_sources.sql: реальная схема raw.r1c_field_writeoff_doc/lines
-- использует doc_ref/line_no (не _id/line_number), а raw.r1c_polya хранит номер поля
-- в колонке description (не naimenovanie). См. коммит 80671e0 (02_raw.sql, 2026-07-06).

-- ============================================================
-- 1. Дополняем существующие колонки под реальный PK (doc_ref)
-- ============================================================
ALTER TABLE raw.r1c_field_writeoff_doc ADD COLUMN IF NOT EXISTS _deletionmark boolean;
ALTER TABLE raw.r1c_field_writeoff_doc ADD COLUMN IF NOT EXISTS _posted boolean;
-- otpravitel_text/poluchatel_text/operaciya/otvetstvennyi_id уже добавлены в 06_new_raw_sources.sql

-- ============================================================
-- 2. Пересоздаём staging-views с правильными именами колонок
-- ============================================================
DROP VIEW IF EXISTS staging.v_tok_vzveshivanie_clean;
DROP VIEW IF EXISTS mart.v_fact_harvest_tok;
DROP VIEW IF EXISTS staging.v_field_writeoff_clean;

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
    ON p.description = staging.extract_field_number_any(t.otkuda_text)  -- FIX: description, не naimenovanie
WHERE COALESCE(t._deletionmark, false) = false;

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

CREATE OR REPLACE VIEW staging.v_field_writeoff_clean AS
SELECT
    d.doc_ref AS _id,                     -- FIX: doc_ref, не _id
    d.doc_number,
    d.doc_date,
    d.otpravitel_text,
    d.poluchatel_text,
    d.operaciya,
    staging.extract_field_number_any(d.otpravitel_text) AS pole_nomer,
    p._id AS pole_id,
    l.nomenklatura_ref AS nomenklatura_id, -- FIX: nomenklatura_ref, не nomenklatura_id
    l.quantity AS kolichestvo,             -- FIX: quantity, не kolichestvo
    l.uom_ref AS edinica_id                -- FIX: uom_ref, не edinica_id
FROM raw.r1c_field_writeoff_doc d
JOIN raw.r1c_field_writeoff_lines l ON l.doc_ref = d.doc_ref   -- FIX: doc_ref, не doc_id
LEFT JOIN raw.r1c_polya p
    ON p.description = staging.extract_field_number_any(d.otpravitel_text)  -- FIX
WHERE COALESCE(d._deletionmark, false) = false;
