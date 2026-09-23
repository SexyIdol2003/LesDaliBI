-- 38_add_semena_to_agro_input_cost_model.sql
-- Добавляем категорию "Семена" в денежную модель агровходов.
-- Документ-источник тот же ("Акт на списания семян, удобрений и ядов"),
-- просто раньше фильтр по АпкНазначение не включал "Семена".

CREATE OR REPLACE VIEW mart.v_agro_input_purchase_price AS
SELECT
    l.doc_id                              AS document_ref_key,
    h.doc_number                          AS document_number,
    h.doc_date::date                      AS document_date,
    h.organizaciya_id::text               AS organization_key,
    h.kontragent_id::text                 AS contractor_key,
    l.line_number,
    l.nomenklatura_id::text               AS nomenklatura_key,
    n."Description"                       AS nomenklatura,
    CASE
        WHEN n."АпкНазначение" = 'Удобрение' THEN 'Удобрение'
        WHEN n."АпкНазначение" = 'СЗР'        THEN 'СЗР'
        WHEN n."АпкНазначение" = 'Семена'      THEN 'Семена'
        ELSE 'Другое'
    END                                    AS agro_category_1c,
    n."АпкНазначение"                     AS apk_assignment_source,
    n."АпкВидУдобрений"                   AS fertilizer_type_source,
    n."АпкКлассПродукции"                 AS product_class_source,
    n."АпкСодержаниеАзота"                AS nitrogen_content_source,
    n."АпкСодержаниеФосфора"              AS phosphorus_content_source,
    n."АпкСодержаниеКалия"                AS potassium_content_source,
    l.harakteristika_id::text             AS characteristic_key,
    l.seriya_id::text                     AS series_key,
    l.sklad_id::text                      AS warehouse_key,
    l.kolichestvo                         AS quantity,
    l.cena                                AS price_no_vat,
    l.summa                               AS amount_no_vat,
    l.stavka_nds_id::text                 AS vat_rate_key,
    l.summa_nds                           AS vat_amount,
    l.summa_s_nds                         AS amount_with_vat,
    h.cena_vklyuchaet_nds                 AS price_includes_vat,
    CASE
        WHEN l.kolichestvo > 0 THEN ROUND(l.summa / l.kolichestvo, 6)
        ELSE NULL
    END                                    AS effective_price_no_vat,
    CASE
        WHEN l.kolichestvo > 0 AND l.cena IS NOT NULL
            THEN ROUND(l.summa - l.kolichestvo * l.cena, 6)
        ELSE NULL
    END                                    AS amount_price_delta,
    CASE
        WHEN l.kolichestvo > 0 AND l.cena IS NOT NULL AND l.summa <> 0
            THEN ROUND(ABS(l.summa - l.kolichestvo * l.cena) / ABS(l.summa) * 100, 4)
        ELSE NULL
    END                                    AS amount_price_delta_pct,
    CASE
        WHEN LOWER(n."Description") LIKE '%аммиачн%селитр%'
             AND COALESCE(n."АпкВидУдобрений", '') <> 'Минеральные'
            THEN true
        ELSE false
    END                                    AS is_classification_suspect,
    CASE
        WHEN l.kolichestvo IS NULL OR l.kolichestvo <= 0 THEN 'НЕТ КОЛИЧЕСТВА'
        WHEN l.summa IS NULL THEN 'НЕТ СУММЫ'
        WHEN l.cena IS NULL OR l.cena = 0 THEN 'НЕТ ЦЕНЫ В ДОКУМЕНТЕ'
        WHEN ABS(l.summa - l.kolichestvo * l.cena) > GREATEST(10, ABS(l.summa) * 0.01)
            THEN 'ПРОВЕРИТЬ СУММУ И ЦЕНУ'
        ELSE 'OK'
    END                                    AS data_quality_status
FROM raw.r1c_purchase_lines l
JOIN raw.r1c_purchase_headers h
    ON h._id = l.doc_id
JOIN raw.r1c_nomenclature n
    ON n."Ref_Key" = l.nomenklatura_id::text
WHERE n."АпкНазначение" = ANY (ARRAY['СЗР', 'Удобрение', 'Семена']);

GRANT SELECT ON mart.v_agro_input_purchase_price TO datalens_ro;

-- Обновляем денежную помесячную витрину — категория теперь включает "Семена"
CREATE OR REPLACE VIEW mart.v_agro_input_cost_per_field_month AS
WITH writeoff_base AS (
    SELECT
        d._id                                   AS document_id,
        d.doc_number,
        d.doc_date::date                         AS document_date,
        d.apk_obekt_zatrat_id                    AS apk_cost_object_key,
        l.line_number,
        l.nomenklatura_id,
        l.kolichestvo                            AS quantity,
        l.apk_ploshchad_obrabotannaya             AS processed_area_ha
    FROM raw.r1c_field_material_writeoff_doc d
    JOIN raw.r1c_field_material_writeoff_lines l
        ON l.doc_id = d._id
    WHERE COALESCE(d._deletionmark, false) = false
      AND COALESCE(d._posted, false) = true
      AND d.doc_date IS NOT NULL
      AND l.nomenklatura_id IS NOT NULL
),
priced AS (
    SELECT
        w.*,
        p.effective_price_no_vat,
        p.document_date                          AS price_document_date,
        p.document_number                        AS price_document_number,
        CASE
            WHEN p.document_date IS NULL THEN 'НЕТ ЦЕНЫ'
            WHEN p.document_date <= w.document_date THEN 'OK'
            ELSE 'ЦЕНА ПОСЛЕ ДАТЫ СПИСАНИЯ'
        END                                       AS data_quality_status
    FROM writeoff_base w
    LEFT JOIN LATERAL (
        SELECT pp.effective_price_no_vat, pp.document_date, pp.document_number
        FROM mart.v_agro_input_purchase_price pp
        WHERE pp.nomenklatura_key = w.nomenklatura_id::text
          AND pp.data_quality_status = 'OK'
        ORDER BY
            (pp.document_date <= w.document_date) DESC,
            ABS(pp.document_date - w.document_date) ASC
        LIMIT 1
    ) p ON true
),
costed AS (
    SELECT
        pr.*,
        pr.quantity * pr.effective_price_no_vat  AS material_cost_no_vat,
        m.field_sk
    FROM priced pr
    LEFT JOIN staging.map_apk_cost_object_to_field m
        ON m.apk_cost_object_key = pr.apk_cost_object_key
       AND pr.document_date BETWEEN m.valid_from AND m.valid_to
       AND m.is_active = true
    WHERE pr.data_quality_status = 'OK'
      AND pr.effective_price_no_vat IS NOT NULL
),
by_field_month AS (
    SELECT
        date_trunc('month', c.document_date)::date  AS period_month,
        EXTRACT(YEAR FROM c.document_date)::integer AS season,
        EXTRACT(MONTH FROM c.document_date)::integer AS month_num,
        c.field_sk,
        CASE
            WHEN nn."АпкНазначение" = 'Удобрение' THEN 'Удобрение'
            WHEN nn."АпкНазначение" = 'СЗР'        THEN 'СЗР'
            WHEN nn."АпкНазначение" = 'Семена'      THEN 'Семена'
            ELSE 'Другое'
        END                                        AS category,
        COUNT(*)                                  AS material_lines,
        SUM(c.material_cost_no_vat)                AS total_cost_rub_no_vat
    FROM costed c
    LEFT JOIN raw.r1c_nomenclature nn
        ON nn."Ref_Key" = c.nomenklatura_id::text
    WHERE c.field_sk IS NOT NULL
    GROUP BY
        date_trunc('month', c.document_date)::date,
        EXTRACT(YEAR FROM c.document_date)::integer,
        EXTRACT(MONTH FROM c.document_date)::integer,
        c.field_sk,
        CASE
            WHEN nn."АпкНазначение" = 'Удобрение' THEN 'Удобрение'
            WHEN nn."АпкНазначение" = 'СЗР'        THEN 'СЗР'
            WHEN nn."АпкНазначение" = 'Семена'      THEN 'Семена'
            ELSE 'Другое'
        END
),
field_area_by_season AS (
    SELECT
        h.god_urozhaya::integer AS season,
        d.field_sk,
        MAX(h.field_name) AS field_name,
        MAX(h.area_ha)    AS field_area_ha
    FROM mart.v_field_area_by_season h
    JOIN mart.dim_field d
        ON d.field_code_1c = h.pole_id::text
       AND d.is_current = true
    WHERE h.area_ha > 0
    GROUP BY h.god_urozhaya::integer, d.field_sk
)
SELECT
    c.period_month,
    c.season,
    c.month_num,
    TRIM(TO_CHAR(c.period_month, 'TMMonth')) AS month_name,
    c.field_sk,
    fa.field_name,
    c.category,
    c.material_lines,
    ROUND(c.total_cost_rub_no_vat, 2)         AS total_cost_rub_no_vat,
    ROUND(fa.field_area_ha, 2)                AS field_area_ha,
    ROUND(c.total_cost_rub_no_vat / NULLIF(fa.field_area_ha, 0), 2) AS cost_rub_per_field_ha,
    'OK: только подтвержденные цены'::text    AS data_quality_status
FROM by_field_month c
JOIN field_area_by_season fa
    ON fa.season = c.season
   AND fa.field_sk = c.field_sk;

GRANT SELECT ON mart.v_agro_input_cost_per_field_month TO datalens_ro;
