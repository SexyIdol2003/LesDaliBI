-- 41_agro_input_cost_per_processed_ha.sql
-- Историческая стоимость СЗР / удобрений / семян на фактически обработанный гектар.
--
-- ВАЖНО:
-- 1. apk_ploshchad_obrabotannaya повторяется в каждой товарной строке одного акта,
--    поэтому площадь считается один раз на документ + объект затрат + категорию.
-- 2. В смешанных актах одна и та же площадь может быть у СЗР и удобрений.
--    Показатель по категории корректен сам по себе, но площади категорий нельзя
--    складывать между собой как независимые гектары.
-- 3. Для показателя TOTAL площадь считается один раз на документ/объект затрат,
--    чтобы не задваивать гектары смешанного акта.

CREATE OR REPLACE VIEW mart.v_agro_input_cost_per_processed_ha_month AS
WITH writeoff_base AS (
    SELECT
        d._id AS document_id,
        d.doc_number,
        d.doc_date::date AS document_date,
        d.apk_obekt_zatrat_id AS apk_cost_object_key,
        l.line_number,
        l.nomenklatura_id,
        l.kolichestvo AS quantity,
        l.apk_ploshchad_obrabotannaya AS processed_area_ha
    FROM raw.r1c_field_material_writeoff_doc d
    JOIN raw.r1c_field_material_writeoff_lines l
      ON l.doc_id = d._id
    WHERE COALESCE(d._deletionmark, false) = false
      AND COALESCE(d._posted, false) = true
      AND d.doc_date IS NOT NULL
      AND l.nomenklatura_id IS NOT NULL
      AND l.apk_ploshchad_obrabotannaya > 0
),
priced AS (
    SELECT
        w.*,
        p.effective_price_no_vat,
        p.document_date AS price_document_date,
        CASE
            WHEN p.document_date IS NULL THEN 'НЕТ ЦЕНЫ'
            WHEN p.document_date <= w.document_date THEN 'OK'
            ELSE 'ЦЕНА ПОСЛЕ ДАТЫ СПИСАНИЯ'
        END AS price_quality_status
    FROM writeoff_base w
    LEFT JOIN LATERAL (
        SELECT
            pp.effective_price_no_vat,
            pp.document_date
        FROM mart.v_agro_input_purchase_price pp
        WHERE pp.nomenklatura_key = w.nomenklatura_id::text
          AND pp.data_quality_status = 'OK'
        ORDER BY
            (pp.document_date <= w.document_date) DESC,
            ABS(pp.document_date - w.document_date) ASC
        LIMIT 1
    ) p ON true
),
costed_lines AS (
    SELECT
        p.document_id,
        p.doc_number,
        p.document_date,
        p.apk_cost_object_key,
        p.nomenklatura_id,
        p.processed_area_ha,
        m.field_sk,
        CASE
            WHEN n."АпкНазначение" = 'Удобрение' THEN 'Удобрение'
            WHEN n."АпкНазначение" = 'СЗР' THEN 'СЗР'
            WHEN n."АпкНазначение" = 'Семена' THEN 'Семена'
            ELSE 'Другое'
        END AS category,
        p.quantity * p.effective_price_no_vat AS material_cost_no_vat
    FROM priced p
    LEFT JOIN raw.r1c_nomenclature n
      ON n."Ref_Key" = p.nomenklatura_id::text
    LEFT JOIN staging.map_apk_cost_object_to_field m
      ON m.apk_cost_object_key = p.apk_cost_object_key
     AND p.document_date BETWEEN m.valid_from AND m.valid_to
     AND m.is_active = true
    WHERE p.price_quality_status = 'OK'
      AND p.effective_price_no_vat IS NOT NULL
      AND n."АпкНазначение" = ANY (ARRAY['СЗР', 'Удобрение', 'Семена'])
),
document_category AS (
    SELECT
        document_id,
        doc_number,
        document_date,
        apk_cost_object_key,
        field_sk,
        category,
        MAX(processed_area_ha) AS processed_area_ha,
        SUM(material_cost_no_vat) AS total_cost_rub_no_vat,
        COUNT(*) AS material_lines
    FROM costed_lines
    GROUP BY
        document_id,
        doc_number,
        document_date,
        apk_cost_object_key,
        field_sk,
        category
),
category_month AS (
    SELECT
        date_trunc('month', document_date)::date AS period_month,
        EXTRACT(YEAR FROM document_date)::integer AS season,
        EXTRACT(MONTH FROM document_date)::integer AS month_num,
        field_sk,
        category,
        COUNT(*) AS documents,
        SUM(material_lines) AS material_lines,
        SUM(total_cost_rub_no_vat) AS total_cost_rub_no_vat,
        SUM(processed_area_ha) AS processed_area_ha
    FROM document_category
    WHERE field_sk IS NOT NULL
    GROUP BY
        date_trunc('month', document_date)::date,
        EXTRACT(YEAR FROM document_date)::integer,
        EXTRACT(MONTH FROM document_date)::integer,
        field_sk,
        category
),
document_total AS (
    SELECT
        document_id,
        document_date,
        apk_cost_object_key,
        field_sk,
        MAX(processed_area_ha) AS processed_area_ha,
        SUM(total_cost_rub_no_vat) AS total_cost_rub_no_vat,
        SUM(material_lines) AS material_lines
    FROM document_category
    GROUP BY
        document_id,
        document_date,
        apk_cost_object_key,
        field_sk
),
total_month AS (
    SELECT
        date_trunc('month', document_date)::date AS period_month,
        EXTRACT(YEAR FROM document_date)::integer AS season,
        EXTRACT(MONTH FROM document_date)::integer AS month_num,
        field_sk,
        'ВСЕГО'::text AS category,
        COUNT(*) AS documents,
        SUM(material_lines) AS material_lines,
        SUM(total_cost_rub_no_vat) AS total_cost_rub_no_vat,
        SUM(processed_area_ha) AS processed_area_ha
    FROM document_total
    WHERE field_sk IS NOT NULL
    GROUP BY
        date_trunc('month', document_date)::date,
        EXTRACT(YEAR FROM document_date)::integer,
        EXTRACT(MONTH FROM document_date)::integer,
        field_sk
),
all_rows AS (
    SELECT * FROM category_month
    UNION ALL
    SELECT * FROM total_month
)
SELECT
    a.period_month,
    a.season,
    a.month_num,
    TRIM(TO_CHAR(a.period_month, 'TMMonth')) AS month_name,
    a.field_sk,
    COALESCE(df.field_name, 'Поле не найдено') AS field_name,
    a.category,
    a.documents,
    a.material_lines,
    ROUND(a.total_cost_rub_no_vat, 2) AS total_cost_rub_no_vat,
    ROUND(a.processed_area_ha, 2) AS processed_area_ha,
    ROUND(
        a.total_cost_rub_no_vat / NULLIF(a.processed_area_ha, 0),
        2
    ) AS cost_rub_per_processed_ha,
    CASE
        WHEN a.category = 'ВСЕГО'
            THEN 'OK: площадь уникальна на акт/поле'
        ELSE 'OK: площадь уникальна на акт/поле/категорию'
    END AS data_quality_status
FROM all_rows a
LEFT JOIN mart.dim_field df
  ON df.field_sk = a.field_sk
 AND df.is_current = true;

GRANT SELECT ON mart.v_agro_input_cost_per_processed_ha_month TO datalens_ro;
