-- ============================================================================
-- 57_mart_field_agro_input_usage_direct.sql
--
-- Прямое фактическое использование семян, удобрений и СЗР по полям.
--
-- Источник:
--   1С OData → Document_ДвижениеПродукцииИМатериалов
--   АпкВидДокумента = АктНаСписанияСемянУдобренийИЯдов.
--
-- Гранулярность:
--   дата акта × поле × номенклатура × строка акта.
--
-- Поле:
--   АпкОбъектЗатрат_Key → staging.map_apk_cost_object_to_field → mart.dim_field.
--
-- Важно:
--   * Цена и сумма в строках акта сейчас равны нулю во всех загруженных строках.
--   * Эта витрина отражает фактическое натуральное использование и прямую
--     field-level связь, но не финансовую стоимость.
--   * Денежная стоимость будет добавлена отдельным слоем через цены закупок
--     с явным статусом качества цены.
-- ============================================================================

CREATE OR REPLACE VIEW mart.v_field_agro_input_usage_direct AS
SELECT
    d._id::text AS document_id,
    d.doc_number,
    d.doc_date::date AS document_date,
    date_trunc('month', d.doc_date)::date AS period_month,
    EXTRACT(YEAR FROM d.doc_date)::integer AS season_year,

    d.apk_obekt_zatrat_id::text AS apk_cost_object_key,
    m.field_sk,
    df.field_code_1c::text AS pole_id,
    df.field_name,

    d.apk_vid_raboty_id::text AS agr_operaciya_id,

    l.line_number,
    l.nomenklatura_id::text AS nomenklatura_id,
    n.description AS nomenklatura_name,
    n."АпкНазначение" AS agro_category_source,

    CASE
        WHEN n."АпкНазначение" = 'Семена' THEN 'SEEDS'
        WHEN n."АпкНазначение" = 'Удобрение' THEN 'FERTILIZERS'
        WHEN n."АпкНазначение" = 'СЗР' THEN 'CROP_PROTECTION'
        WHEN n._id IS NULL THEN 'UNMATCHED_NOMENCLATURE'
        ELSE 'OTHER_AGRO_INPUT'
    END AS agro_input_category,

    l.gruppa_produkcii_id::text AS product_group_id,
    l.harakteristika_id::text AS characteristic_id,
    l.seriya_id::text AS series_id,

    l.kolichestvo AS quantity,
    l.kolichestvo_upakovok AS packages_quantity,
    l.apk_ploshchad_obrabotannaya AS processed_area_ha,
    l.apk_raskhod_na_ga AS application_rate_per_ha,

    l.cena AS document_price_rub,
    l.summa AS document_amount_rub,

    CASE
        WHEN m.field_sk IS NULL THEN 'NO_FIELD_MAPPING'
        WHEN n._id IS NULL THEN 'NO_NOMENCLATURE_MATCH'
        WHEN n."АпкНазначение" IN ('Семена', 'Удобрение', 'СЗР')
            THEN 'OK_CLASSIFIED_AGRO_INPUT'
        ELSE 'UNCLASSIFIED_OR_OTHER_NOMENCLATURE'
    END AS agro_input_quality_status,

    CASE
        WHEN l.summa IS NULL OR l.summa = 0
            THEN 'NO_DOCUMENT_COST'
        ELSE 'DOCUMENT_COST_AVAILABLE'
    END AS document_cost_status,

    'ACTUAL_FIELD_WRITE_OFF_DOCUMENT'::text AS source_method,
    now() AS calculated_at
FROM raw.r1c_field_material_writeoff_doc d
JOIN raw.r1c_field_material_writeoff_lines l
  ON l.doc_id = d._id
LEFT JOIN staging.map_apk_cost_object_to_field m
  ON m.apk_cost_object_key = d.apk_obekt_zatrat_id
 AND d.doc_date::date BETWEEN m.valid_from AND m.valid_to
 AND m.is_active = true
LEFT JOIN mart.dim_field df
  ON df.field_sk = m.field_sk
 AND df.is_current = true
LEFT JOIN raw.r1c_nomenclature n
  ON n._id::text = l.nomenklatura_id::text
WHERE d.apk_vid_dokumenta = 'АктНаСписанияСемянУдобренийИЯдов'
  AND COALESCE(d._posted, false) = true
  AND COALESCE(d._deletionmark, false) = false;

COMMENT ON VIEW mart.v_field_agro_input_usage_direct IS
    'Фактическое использование семян, удобрений и СЗР по полям из актов списания. Содержит количество, площадь обработки, норму и field mapping. Документная стоимость пока отсутствует в OData и не подставляется.';

GRANT SELECT ON mart.v_field_agro_input_usage_direct TO datalens_ro;
