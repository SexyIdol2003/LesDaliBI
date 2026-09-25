-- ============================================================================
-- 58_mart_field_agro_input_cost_direct.sql
--
-- Расчётная стоимость фактических списаний семян, удобрений и СЗР на поле.
--
-- Факт использования:
--   mart.v_field_agro_input_usage_direct
--   (акт списания семян, удобрений и ядов; конкретное поле/объект затрат).
--
-- Цена:
--   mart.v_agro_input_purchase_price.effective_price_no_vat.
--
-- Строгое правило цены:
--   В strict_cost_rub_no_vat включается только цена закупки, датированная
--   не позднее даты списания: PURCHASE_PRICE_ON_OR_BEFORE_WRITEOFF.
--
-- Цена после даты списания сохраняется отдельно как diagnostic_estimated_cost,
-- но не включается в strict cost и не может использоваться в итоговой СС на га.
--
-- Документные цены/суммы сейчас нулевые у всех строк OData, поэтому не
-- используются как финансовый источник.
--
-- ИСПРАВЛЕНО 2026-09-25: добавлен отдельный статус
-- OWN_SEED_COST_NOT_AVAILABLE_IN_EXTRACT для собственных семян
-- ("Семена собств..."), чтобы не путать их с обычным NO_PURCHASE_PRICE.
-- Задеплоено в прод, проверено: OK_STRICT_COST=4223,
-- PRICE_AFTER_WRITEOFF_NOT_IN_STRICT_COST=291,
-- OWN_SEED_COST_NOT_AVAILABLE_IN_EXTRACT=17, NO_PURCHASE_PRICE=13.
-- ============================================================================

CREATE OR REPLACE VIEW mart.v_field_agro_input_cost_direct AS
WITH base AS (
    SELECT
        u.*,
        p.document_date AS price_document_date,
        p.effective_price_no_vat,
        p.data_quality_status AS purchase_price_quality_status
    FROM mart.v_field_agro_input_usage_direct u
    LEFT JOIN LATERAL (
        SELECT
            pp.document_date,
            pp.effective_price_no_vat,
            pp.data_quality_status
        FROM mart.v_agro_input_purchase_price pp
        WHERE pp.nomenklatura_key = u.nomenklatura_id
          AND pp.data_quality_status = 'OK'
        ORDER BY
            (pp.document_date <= u.document_date) DESC,
            ABS(pp.document_date - u.document_date) ASC
        LIMIT 1
    ) p ON true
    WHERE u.agro_input_category IN (
        'SEEDS',
        'FERTILIZERS',
        'CROP_PROTECTION'
    )
),
classified AS (
    SELECT
        b.*,
        CASE
            WHEN b.effective_price_no_vat IS NULL
                THEN 'NO_PURCHASE_PRICE'
            WHEN b.price_document_date <= b.document_date
                THEN 'PURCHASE_PRICE_ON_OR_BEFORE_WRITEOFF'
            ELSE 'ONLY_PRICE_AFTER_WRITEOFF'
        END AS purchase_price_match_status,

        CASE
            WHEN b.effective_price_no_vat IS NOT NULL
                THEN b.quantity * b.effective_price_no_vat
            ELSE NULL::numeric
        END AS diagnostic_estimated_cost_rub_no_vat,

        CASE
            WHEN b.effective_price_no_vat IS NOT NULL
             AND b.price_document_date <= b.document_date
                THEN b.quantity * b.effective_price_no_vat
            ELSE NULL::numeric
        END AS strict_cost_rub_no_vat
    FROM base b
)
SELECT
    document_id,
    doc_number,
    document_date,
    period_month,
    season_year,

    apk_cost_object_key,
    field_sk,
    pole_id,
    field_name,
    agr_operaciya_id,

    line_number,
    nomenklatura_id,
    nomenklatura_name,
    agro_category_source,
    agro_input_category,

    product_group_id,
    characteristic_id,
    series_id,

    quantity,
    packages_quantity,
    processed_area_ha,
    application_rate_per_ha,

    price_document_date,
    effective_price_no_vat,
    purchase_price_quality_status,
    purchase_price_match_status,

    ROUND(diagnostic_estimated_cost_rub_no_vat, 2)
        AS diagnostic_estimated_cost_rub_no_vat,
    ROUND(strict_cost_rub_no_vat, 2)
        AS strict_cost_rub_no_vat,

    CASE
        WHEN strict_cost_rub_no_vat IS NOT NULL
            THEN 'OK_STRICT_COST'
        WHEN purchase_price_match_status = 'ONLY_PRICE_AFTER_WRITEOFF'
            THEN 'PRICE_AFTER_WRITEOFF_NOT_IN_STRICT_COST'
        WHEN agro_input_category = 'SEEDS'
         AND lower(COALESCE(nomenklatura_name, '')) LIKE 'семена собств%'
            THEN 'OWN_SEED_COST_NOT_AVAILABLE_IN_EXTRACT'
        WHEN purchase_price_match_status = 'NO_PURCHASE_PRICE'
            THEN 'NO_PURCHASE_PRICE'
        ELSE 'CHECK_AGRO_PRICE'
    END AS agro_cost_quality_status,

    agro_input_quality_status,
    document_cost_status,
    source_method,

    'PURCHASE_PRICE_BEFORE_WRITEOFF'::text AS cost_method,
    now() AS calculated_at
FROM classified;

COMMENT ON VIEW mart.v_field_agro_input_cost_direct IS
    'Расчётная field-level стоимость фактических списаний семян, удобрений и СЗР. В strict_cost_rub_no_vat включаются только закупочные цены на дату не позже списания. Собственные семена помечены статусом OWN_SEED_COST_NOT_AVAILABLE_IN_EXTRACT. Цена после списания сохраняется диагностически и не включается в строгую СС.';

GRANT SELECT ON mart.v_field_agro_input_cost_direct TO datalens_ro;
