-- Apply after files/59_fix_agro_purchase_price_catalog_coverage.sql in one transaction.
-- 60: preserve the historical monthly mart's legacy-catalogue scope.
CREATE OR REPLACE VIEW mart.v_agro_input_cost_per_field_month AS
WITH writeoff_base AS (
    SELECT d._id AS document_id, d.doc_number, d.doc_date::date AS document_date,
           d.apk_obekt_zatrat_id AS apk_cost_object_key, l.line_number,
           l.nomenklatura_id, l.kolichestvo AS quantity,
           l.apk_ploshchad_obrabotannaya AS processed_area_ha
    FROM raw.r1c_field_material_writeoff_doc d
    JOIN raw.r1c_field_material_writeoff_lines l ON l.doc_id = d._id
    WHERE COALESCE(d._deletionmark, false) = false
      AND COALESCE(d._posted, false) = true
      AND d.doc_date IS NOT NULL AND l.nomenklatura_id IS NOT NULL
), priced AS (
    SELECT w.*, p.effective_price_no_vat, p.document_date AS price_document_date,
           p.document_number AS price_document_number,
           CASE WHEN p.document_date IS NULL THEN 'НЕТ ЦЕНЫ'
                WHEN p.document_date <= w.document_date THEN 'OK'
                ELSE 'ЦЕНА ПОСЛЕ ДАТЫ СПИСАНИЯ' END AS price_quality_status
    FROM writeoff_base w
    LEFT JOIN LATERAL (
        SELECT pp.effective_price_no_vat, pp.document_date, pp.document_number
        FROM mart.v_agro_input_purchase_price pp
        WHERE pp.nomenklatura_key = w.nomenklatura_id::text
          AND pp.data_quality_status = 'OK'
          AND EXISTS (
              SELECT 1 FROM raw.r1c_nomenclature legacy
              WHERE legacy._id = w.nomenklatura_id::text
          )
        ORDER BY (pp.document_date <= w.document_date) DESC,
                 abs(pp.document_date - w.document_date)
        LIMIT 1
    ) p ON true
), costed AS (
    SELECT pr.*, pr.quantity * pr.effective_price_no_vat AS material_cost_no_vat,
           m.field_sk
    FROM priced pr
    LEFT JOIN staging.map_apk_cost_object_to_field m
      ON m.apk_cost_object_key = pr.apk_cost_object_key
     AND pr.document_date BETWEEN m.valid_from AND m.valid_to
     AND m.is_active = true
    WHERE pr.price_quality_status = 'OK' AND pr.effective_price_no_vat IS NOT NULL
), by_field_month AS (
    SELECT date_trunc('month', c.document_date)::date AS period_month,
           EXTRACT(YEAR FROM c.document_date)::integer AS season,
           EXTRACT(MONTH FROM c.document_date)::integer AS month_num,
           c.field_sk,
           CASE WHEN n."АпкНазначение" = 'Удобрение' THEN 'Удобрение'
                WHEN n."АпкНазначение" = 'СЗР' THEN 'СЗР'
                WHEN n."АпкНазначение" = 'Семена' THEN 'Семена'
                ELSE 'Другое' END AS category,
           count(*) AS material_lines, sum(c.material_cost_no_vat) AS total_cost_rub_no_vat
    FROM costed c
    LEFT JOIN raw.r1c_nomenclature n ON n._id = c.nomenklatura_id::text
    WHERE c.field_sk IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
), field_area_by_season AS (
    SELECT h.god_urozhaya AS season, d.field_sk,
           max(h.field_name) AS field_name, max(h.area_ha) AS field_area_ha
    FROM mart.v_field_area_by_season h
    JOIN mart.dim_field d ON d.field_code_1c = h.pole_id AND d.is_current = true
    WHERE h.area_ha > 0
    GROUP BY h.god_urozhaya, d.field_sk
)
SELECT c.period_month, c.season, c.month_num,
       trim(to_char(c.period_month, 'TMMonth')) AS month_name,
       c.field_sk,
       COALESCE(fa.field_name, df.field_name, 'Поле не найдено') AS field_name,
       c.category, c.material_lines, round(c.total_cost_rub_no_vat, 2) AS total_cost_rub_no_vat,
       round(fa.field_area_ha, 2) AS field_area_ha,
       round(c.total_cost_rub_no_vat / NULLIF(fa.field_area_ha, 0), 2) AS cost_rub_per_field_ha,
       CASE WHEN fa.field_area_ha IS NULL THEN 'НЕТ ПЛОЩАДИ ПОЛЯ В СЕЗОНЕ'
            ELSE 'OK' END AS data_quality_status
FROM by_field_month c
LEFT JOIN field_area_by_season fa ON fa.season = c.season AND fa.field_sk = c.field_sk
LEFT JOIN mart.dim_field df ON df.field_sk = c.field_sk AND df.is_current = true;

-- 61: use legacy assignment first and the current catalogue's classifier only for missing legacy IDs.
CREATE OR REPLACE VIEW mart.v_field_agro_input_usage_direct AS
SELECT
    d._id::text AS document_id, d.doc_number, d.doc_date::date AS document_date,
    date_trunc('month', d.doc_date)::date AS period_month,
    EXTRACT(YEAR FROM d.doc_date)::integer AS season_year,
    d.apk_obekt_zatrat_id::text AS apk_cost_object_key,
    m.field_sk, df.field_code_1c::text AS pole_id, df.field_name,
    d.apk_vid_raboty_id::text AS agr_operaciya_id,
    l.line_number, l.nomenklatura_id::text AS nomenklatura_id,
    COALESCE(n.description, nc.description) AS nomenklatura_name,
    n."АпкНазначение" AS agro_category_source,
    CASE
        WHEN n."АпкНазначение" = 'Семена' OR (n._id IS NULL AND cls.input_category = 'Семена') THEN 'SEEDS'
        WHEN n."АпкНазначение" = 'Удобрение' OR (n._id IS NULL AND cls.input_category = 'Удобрение') THEN 'FERTILIZERS'
        WHEN n."АпкНазначение" = 'СЗР' OR (n._id IS NULL AND cls.input_category = 'СЗР') THEN 'CROP_PROTECTION'
        WHEN n._id IS NULL AND nc._id IS NULL THEN 'UNMATCHED_NOMENCLATURE'
        ELSE 'OTHER_AGRO_INPUT'
    END AS agro_input_category,
    l.gruppa_produkcii_id::text AS product_group_id,
    l.harakteristika_id::text AS characteristic_id,
    l.seriya_id::text AS series_id,
    l.kolichestvo AS quantity, l.kolichestvo_upakovok AS packages_quantity,
    l.apk_ploshchad_obrabotannaya AS processed_area_ha,
    l.apk_raskhod_na_ga AS application_rate_per_ha,
    l.cena AS document_price_rub, l.summa AS document_amount_rub,
    CASE
        WHEN m.field_sk IS NULL THEN 'NO_FIELD_MAPPING'
        WHEN n._id IS NULL AND nc._id IS NULL THEN 'NO_NOMENCLATURE_MATCH'
        WHEN n."АпкНазначение" IN ('Семена', 'Удобрение', 'СЗР')
          OR (n._id IS NULL AND cls.input_category IN ('Семена', 'Удобрение', 'СЗР'))
            THEN 'OK_CLASSIFIED_AGRO_INPUT'
        ELSE 'UNCLASSIFIED_OR_OTHER_NOMENCLATURE'
    END AS agro_input_quality_status,
    CASE WHEN l.summa IS NULL OR l.summa = 0 THEN 'NO_DOCUMENT_COST'
         ELSE 'DOCUMENT_COST_AVAILABLE' END AS document_cost_status,
    'ACTUAL_FIELD_WRITE_OFF_DOCUMENT'::text AS source_method,
    now() AS calculated_at
FROM raw.r1c_field_material_writeoff_doc d
JOIN raw.r1c_field_material_writeoff_lines l ON l.doc_id = d._id
LEFT JOIN staging.map_apk_cost_object_to_field m
  ON m.apk_cost_object_key = d.apk_obekt_zatrat_id
 AND d.doc_date::date BETWEEN m.valid_from AND m.valid_to AND m.is_active = true
LEFT JOIN mart.dim_field df ON df.field_sk = m.field_sk AND df.is_current = true
LEFT JOIN raw.r1c_nomenclature n ON n._id = l.nomenklatura_id::text
LEFT JOIN raw.r1c_nomenclature_current nc ON nc._id = l.nomenklatura_id::text
LEFT JOIN LATERAL (
    SELECT c.input_category FROM raw.v_purchase_nomenclature_classification c
    WHERE c.nomenklatura_id = l.nomenklatura_id::text
    ORDER BY c.input_category LIMIT 1
) cls ON true
WHERE d.apk_vid_dokumenta = 'АктНаСписанияСемянУдобренийИЯдов'
  AND COALESCE(d._posted, false) = true
  AND COALESCE(d._deletionmark, false) = false;

-- 62: keep every classified fact; diagnostics may show price, strict cost must exclude deprecated names.
CREATE OR REPLACE VIEW mart.v_field_agro_input_cost_direct AS
WITH base AS (
    SELECT u.*, p.document_date AS price_document_date,
           p.effective_price_no_vat,
           p.data_quality_status AS purchase_price_quality_status
    FROM mart.v_field_agro_input_usage_direct u
    LEFT JOIN LATERAL (
        SELECT pp.document_date, pp.effective_price_no_vat, pp.data_quality_status
        FROM mart.v_agro_input_purchase_price pp
        WHERE pp.nomenklatura_key = u.nomenklatura_id
          AND pp.data_quality_status = 'OK'
          AND (
              EXISTS (
                  SELECT 1 FROM raw.r1c_nomenclature legacy
                  WHERE legacy._id = u.nomenklatura_id
              )
              OR (
                  pp.characteristic_key IS NOT DISTINCT FROM u.characteristic_id
              )
          )
        ORDER BY (pp.document_date <= u.document_date) DESC,
                 abs(pp.document_date - u.document_date)
        LIMIT 1
    ) p ON true
    WHERE u.agro_input_category IN ('SEEDS', 'FERTILIZERS', 'CROP_PROTECTION')
), classified AS (
    SELECT b.*,
           CASE WHEN b.effective_price_no_vat IS NULL THEN 'NO_PURCHASE_PRICE'
                WHEN b.price_document_date <= b.document_date THEN 'PURCHASE_PRICE_ON_OR_BEFORE_WRITEOFF'
                ELSE 'ONLY_PRICE_AFTER_WRITEOFF' END AS purchase_price_match_status,
           CASE WHEN b.effective_price_no_vat IS NOT NULL
                     THEN b.quantity * b.effective_price_no_vat
                ELSE NULL::numeric END AS diagnostic_estimated_cost_rub_no_vat,
           CASE WHEN b.effective_price_no_vat IS NOT NULL
                     AND b.price_document_date <= b.document_date
                     AND COALESCE(b.nomenklatura_name, '') !~* '^\s*\(не использовать\)'
                     THEN b.quantity * b.effective_price_no_vat
                ELSE NULL::numeric END AS strict_cost_rub_no_vat
    FROM base b
)
SELECT document_id, doc_number, document_date, period_month, season_year,
       apk_cost_object_key, field_sk, pole_id, field_name, agr_operaciya_id,
       line_number, nomenklatura_id, nomenklatura_name, agro_category_source,
       agro_input_category, product_group_id, characteristic_id, series_id,
       quantity, packages_quantity, processed_area_ha, application_rate_per_ha,
       price_document_date, effective_price_no_vat, purchase_price_quality_status,
       purchase_price_match_status,
       round(diagnostic_estimated_cost_rub_no_vat, 2) AS diagnostic_estimated_cost_rub_no_vat,
       round(strict_cost_rub_no_vat, 2) AS strict_cost_rub_no_vat,
       CASE
           WHEN COALESCE(nomenklatura_name, '') ~* '^\s*\(не использовать\)'
                THEN 'DO_NOT_USE_REVIEW_REQUIRED'
           WHEN strict_cost_rub_no_vat IS NOT NULL THEN 'OK_STRICT_COST'
           WHEN purchase_price_match_status = 'ONLY_PRICE_AFTER_WRITEOFF'
                THEN 'PRICE_AFTER_WRITEOFF_NOT_IN_STRICT_COST'
           WHEN agro_input_category = 'SEEDS'
             AND lower(COALESCE(nomenklatura_name, '')) LIKE 'семена собств%'
                THEN 'OWN_SEED_COST_NOT_AVAILABLE_IN_EXTRACT'
           WHEN purchase_price_match_status = 'NO_PURCHASE_PRICE' THEN 'NO_PURCHASE_PRICE'
           ELSE 'CHECK_AGRO_PRICE'
       END AS agro_cost_quality_status,
       agro_input_quality_status, document_cost_status, source_method,
       'PURCHASE_PRICE_BEFORE_WRITEOFF'::text AS cost_method,
       now() AS calculated_at
FROM classified;
