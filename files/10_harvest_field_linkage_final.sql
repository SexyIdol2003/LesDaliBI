-- 10_harvest_field_linkage_final.sql
-- Финальная витрина связи «взвешивание на току → поле».
-- Подтверждено на реальных данных 2021–2025:
-- 5 313 активных документов вида СПоля, 100.0% связаны с raw.r1c_polya.
-- Цепочка: tok.otkuda_id (GUID структуры предприятия)
--        → company_structure._id → description "318, ..."
--        → extract_field_number_any(description) → "318"
--        → polya.description → реальный GUID поля.
--
-- Не используем Талоны.Поле_Key: он NULL / нулевой GUID в 100% из 5 617 строк.

DROP VIEW IF EXISTS mart.v_fact_harvest_tok;
DROP VIEW IF EXISTS staging.v_tok_vzveshivanie_clean;

CREATE OR REPLACE VIEW staging.v_tok_vzveshivanie_clean AS
SELECT
    t._id,
    t.doc_number,
    t.doc_date,
    t.vid_vzveshivaniya,
    t.otkuda_id,
    t.kuda_id,
    t.nomenklatura_id,
    t.voditel_id,
    t.avtomobil_id,
    t.ves_netto_kg,
    t.ves_brutto_kg,
    t.ves_tary_kg,
    cs.description AS otkuda_name,
    staging.extract_field_number_any(cs.description) AS pole_nomer,
    p._id AS pole_id,
    EXTRACT(YEAR FROM t.doc_date)::int AS god_urozhaya,
    CASE
        WHEN p._id IS NOT NULL THEN 'company_structure_description'
        ELSE 'unresolved'
    END AS pole_resolution_method
FROM raw.r1c_tok_vzveshivanie t
LEFT JOIN raw.r1c_company_structure cs
    ON cs._id = t.otkuda_id::text
LEFT JOIN raw.r1c_polya p
    ON p.description = staging.extract_field_number_any(cs.description)
WHERE COALESCE(t._deletionmark, false) = false;

-- BI-ready view: одна строка = одно взвешивание «с поля».
-- Включает только СПоля, исключает 40 записей типа «Перемещение».
CREATE OR REPLACE VIEW mart.v_fact_harvest_tok AS
SELECT
    v._id AS src_doc_id,
    v.doc_number,
    v.doc_date,
    v.god_urozhaya,
    v.pole_id,
    v.pole_nomer,
    v.otkuda_name AS pole_party_name,
    v.nomenklatura_id,
    v.ves_netto_kg AS kolichestvo_kg,
    v.ves_brutto_kg,
    v.ves_tary_kg,
    v.voditel_id,
    v.avtomobil_id,
    v.kuda_id AS warehouse_id,
    v.pole_resolution_method,
    (v.pole_id IS NOT NULL) AS is_pole_resolved
FROM staging.v_tok_vzveshivanie_clean v
WHERE v.vid_vzveshivaniya = 'СПоля';

-- Контрольный результат после применения:
-- SELECT god_urozhaya, count(*) AS rows, count(pole_id) AS resolved,
--        round(100.0 * count(pole_id) / count(*), 1) AS coverage_pct
-- FROM mart.v_fact_harvest_tok GROUP BY 1 ORDER BY 1;
