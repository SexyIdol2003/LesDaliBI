-- Помесячная витрина факта применения агроматериалов.
-- Источник: проведённые документы списания материалов из 1С.
-- ВАЖНО: документы списания не содержат цены/суммы; витрина отражает натуральный факт, а не рублёвые затраты.

CREATE OR REPLACE VIEW mart.v_agro_input_usage_per_field_month AS
WITH base AS (
    SELECT
        date_trunc('month', d.doc_date)::date AS period_month,
        EXTRACT(YEAR FROM d.doc_date)::integer AS season,
        EXTRACT(MONTH FROM d.doc_date)::integer AS month_num,
        d._id AS document_id,
        d.doc_number,
        d.doc_date::date AS document_date,
        d.apk_obekt_zatrat_id AS apk_cost_object_key,
        l.line_number,
        l.nomenklatura_id,
        l.kolichestvo AS quantity,
        l.apk_ploshchad_obrabotannaya AS processed_area_ha,
        l.apk_raskhod_na_ga AS application_rate_per_ha
    FROM raw.r1c_field_material_writeoff_doc d
    JOIN raw.r1c_field_material_writeoff_lines l
        ON l.doc_id = d._id
    WHERE COALESCE(d._deletionmark, false) = false
      AND COALESCE(d._posted, false) = true
      AND d.doc_date IS NOT NULL
),
enriched AS (
    SELECT
        b.*,
        m.field_sk,
        f.field_name,
        CASE
            WHEN m.field_sk IS NULL THEN
                'WARNING: не найден маппинг объекта затрат на поле'
            WHEN b.quantity IS NULL THEN
                'WARNING: не заполнено количество'
            WHEN b.processed_area_ha IS NULL
                 OR b.processed_area_ha <= 0 THEN
                'WARNING: не заполнена площадь обработки'
            ELSE
                'OK: натуральный факт, стоимость в документе отсутствует'
        END AS data_quality_status
    FROM base b
    LEFT JOIN staging.map_apk_cost_object_to_field m
        ON m.apk_cost_object_key = b.apk_cost_object_key
       AND b.document_date BETWEEN m.valid_from AND m.valid_to
       AND m.is_active = true
    LEFT JOIN mart.dim_field f
        ON f.field_sk = m.field_sk
       AND f.is_current = true
)
SELECT
    period_month,
    season,
    month_num,
    TRIM(TO_CHAR(period_month, 'TMMonth')) AS month_name,
    field_sk,
    field_name,
    apk_cost_object_key,
    document_id,
    doc_number,
    document_date,
    line_number,
    nomenklatura_id,
    quantity,
    processed_area_ha,
    application_rate_per_ha,
    data_quality_status
FROM enriched;

GRANT SELECT ON mart.v_agro_input_usage_per_field_month TO datalens_ro;

-- Контроль полноты привязки поля после применения:
-- SELECT EXTRACT(YEAR FROM period_month)::integer AS year,
--        COUNT(*) AS lines,
--        COUNT(*) FILTER (WHERE field_sk IS NOT NULL) AS mapped_lines,
--        COUNT(*) FILTER (WHERE field_sk IS NULL) AS unmapped_lines
-- FROM mart.v_agro_input_usage_per_field_month
-- GROUP BY 1 ORDER BY 1;
