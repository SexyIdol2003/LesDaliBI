-- ============================================================================
-- загрузка mart.fact_fuel_refuel из raw.r1c_zapravochnaya_vedomost(_gsm)
--
-- mart.fact_fuel_refuel уже существует (стар-схема, создана ранее):
--   date_day (FK -> mart.dim_date), equipment_sk (FK -> mart.dim_equipment),
--   fuel_brand_id (uuid, без связи с справочником — хранится как raw uuid),
--   liters, src_doc_ref.
--
-- mart.dim_equipment.code_1c — тот же GUID, что raw.r1c_equipment._id /
-- raw.r1c_zapravochnaya_vedomost.tehnika_id (проверено прямым сравнением значений 2026-09-07).
--
-- Полная перезагрузка (TRUNCATE + INSERT) — объём небольшой (~3800 строк),
-- безопасно запускать повторно при каждой ночной выгрузке.
--
-- Строки, где дата выходит за границы mart.dim_date или техника не найдена
-- в dim_equipment, автоматически отсеиваются INNER JOIN'ом на dim_date
-- и LEFT JOIN'ом на dim_equipment (без техники строка всё равно грузится, equipment_sk = NULL).
-- ============================================================================

BEGIN;

TRUNCATE TABLE mart.fact_fuel_refuel;

INSERT INTO mart.fact_fuel_refuel (date_day, equipment_sk, fuel_brand_id, liters, src_doc_ref)
SELECT
    d.date_day,
    eq.eq_sk,
    gsm.marka_topliva_id,
    gsm.kolichestvo,
    gsm.doc_id::text || ':' || gsm.line_number
FROM raw.r1c_zapravochnaya_vedomost_gsm gsm
JOIN raw.r1c_zapravochnaya_vedomost doc
    ON doc._id = gsm.doc_id
JOIN mart.dim_date d
    ON d.date_day = COALESCE(gsm.line_date, doc.doc_date::date)
LEFT JOIN mart.dim_equipment eq
    ON eq.code_1c = COALESCE(gsm.line_tehnika_id, doc.tehnika_id)::text
WHERE doc._posted = true
  AND (doc._deletionmark IS NULL OR doc._deletionmark = false)
  AND gsm.kolichestvo IS NOT NULL;

COMMIT;

-- Контрольная сверка: сколько строк из gsm не попало в витрину
-- (вне границ dim_date или без kolichestvo):
-- SELECT count(*) FROM raw.r1c_zapravochnaya_vedomost_gsm gsm
-- JOIN raw.r1c_zapravochnaya_vedomost doc ON doc._id = gsm.doc_id
-- WHERE doc._posted = true AND (doc._deletionmark IS NULL OR doc._deletionmark = false)
--   AND (gsm.kolichestvo IS NULL
--        OR NOT EXISTS (SELECT 1 FROM mart.dim_date d WHERE d.date_day = COALESCE(gsm.line_date, doc.doc_date::date)));
