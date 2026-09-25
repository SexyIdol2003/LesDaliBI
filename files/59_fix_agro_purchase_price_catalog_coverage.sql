-- Этап 1: сохранить контракт ценовой view и добавить позиции из текущего справочника.
CREATE OR REPLACE VIEW mart.v_agro_input_purchase_price AS
SELECT
    l.doc_id AS document_ref_key,
    h.doc_number AS document_number,
    h.doc_date::date AS document_date,
    h.organizaciya_id::text AS organization_key,
    h.kontragent_id::text AS contractor_key,
    l.line_number,
    l.nomenklatura_id::text AS nomenklatura_key,
    COALESCE(n.description, nc.description) AS nomenklatura,
    CASE
        WHEN n."АпкНазначение" = 'Удобрение' THEN 'Удобрение'
        WHEN n."АпкНазначение" = 'СЗР' THEN 'СЗР'
        WHEN n."АпкНазначение" = 'Семена' THEN 'Семена'
        WHEN n._id IS NULL
         AND c.input_category IN ('Удобрение', 'СЗР', 'Семена')
            THEN c.input_category
        ELSE 'Другое'
    END AS agro_category_1c,
    n."АпкНазначение" AS apk_assignment_source,
    n."АпкВидУдобрений" AS fertilizer_type_source,
    n."АпкКлассПродукции" AS product_class_source,
    n."АпкСодержаниеАзота" AS nitrogen_content_source,
    n."АпкСодержаниеФосфора" AS phosphorus_content_source,
    n."АпкСодержаниеКалия" AS potassium_content_source,
    l.harakteristika_id::text AS characteristic_key,
    l.seriya_id::text AS series_key,
    l.sklad_id::text AS warehouse_key,
    l.kolichestvo AS quantity,
    l.cena AS price_no_vat,
    l.summa AS amount_no_vat,
    l.stavka_nds_id::text AS vat_rate_key,
    l.summa_nds AS vat_amount,
    l.summa_s_nds AS amount_with_vat,
    h.cena_vklyuchaet_nds AS price_includes_vat,
    CASE WHEN l.kolichestvo > 0
         THEN round(l.summa / l.kolichestvo, 6)
         ELSE NULL::numeric END AS effective_price_no_vat,
    CASE WHEN l.kolichestvo > 0 AND l.cena IS NOT NULL
         THEN round(l.summa - l.kolichestvo * l.cena, 6)
         ELSE NULL::numeric END AS amount_price_delta,
    CASE WHEN l.kolichestvo > 0 AND l.cena IS NOT NULL
               AND l.summa <> 0
         THEN round(abs(l.summa - l.kolichestvo * l.cena)
                    / abs(l.summa) * 100, 4)
         ELSE NULL::numeric END AS amount_price_delta_pct,
    CASE WHEN lower(COALESCE(n.description, nc.description))
                  LIKE '%аммиачн%селитр%'
              AND COALESCE(n."АпкВидУдобрений", '') <> 'Минеральные'
         THEN true ELSE false END AS is_classification_suspect,
    CASE
        WHEN l.kolichestvo IS NULL OR l.kolichestvo <= 0
            THEN 'НЕТ КОЛИЧЕСТВА'
        WHEN l.summa IS NULL THEN 'НЕТ СУММЫ'
        WHEN l.cena IS NULL OR l.cena = 0
            THEN 'НЕТ ЦЕНЫ В ДОКУМЕНТЕ'
        WHEN abs(l.summa - l.kolichestvo * l.cena)
             > GREATEST(10, abs(l.summa) * 0.01)
            THEN 'ПРОВЕРИТЬ СУММУ И ЦЕНУ'
        ELSE 'OK'
    END AS data_quality_status
FROM raw.r1c_purchase_lines l
JOIN raw.r1c_purchase_headers h
  ON h._id = l.doc_id
LEFT JOIN raw.r1c_nomenclature n
  ON n._id = l.nomenklatura_id::text
LEFT JOIN raw.r1c_nomenclature_current nc
  ON nc._id = l.nomenklatura_id::text
LEFT JOIN LATERAL (
    SELECT x.input_category
    FROM raw.v_purchase_nomenclature_classification x
    WHERE x.nomenklatura_id = l.nomenklatura_id::text
    ORDER BY x.input_category
    LIMIT 1
) c ON true
WHERE n."АпкНазначение" IN ('СЗР', 'Удобрение', 'Семена')
   OR (
       n._id IS NULL
       AND c.input_category IN ('СЗР', 'Удобрение', 'Семена')
   );
