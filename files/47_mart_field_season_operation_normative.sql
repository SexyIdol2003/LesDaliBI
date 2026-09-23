-- 47_mart_field_season_operation_normative.sql
-- Производственные работы ПУЛ тракториста по полю, сезону и операции.
--
-- Источник:
--   staging.stg_waybill_field_link
--
-- ВАЖНО:
--   * Включаются только строки с link_status = 'resolved_one_field_active'.
--   * normative_fuel_liters — нормативный, НЕ фактический расход ГСМ.
--   * labor_amount_rub — начисленная зарплата по строкам ПУЛ.
--   * Денежная стоимость топлива и полная себестоимость поля здесь не рассчитываются.
--   * crop_id_raw сохраняется только для диагностики: в источнике смешаны
--     UUID культур, пустые значения и текстовые списки.

CREATE OR REPLACE VIEW mart.v_field_season_operation_normative AS
WITH source AS (
    SELECT
        l.put_list_line_id,
        l.put_list_doc_id,
        l.work_date,
        l.pole_id,
        l.season_year,

        l.analitika_raskhodov_id,
        l.structure_code,
        l.structure_description,

        l.agr_operaciya_id,
        l.vid_raboty_id,
        l.oborudovanie_id,
        l.tip_zatrat,

        l.gektarov,
        l.tonn,
        l.kilometrov,
        l.chasov,
        l.mototochas,
        l.normativny_raskhod_gsm,
        l.labor_amount_rub,

        l.crop_id AS crop_id_raw,

        CASE
            WHEN l.crop_id IS NULL OR btrim(l.crop_id) = ''
                THEN 'crop_missing'
            WHEN l.crop_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                THEN 'crop_uuid'
            ELSE 'crop_non_uuid_text'
        END AS crop_quality_status
    FROM staging.stg_waybill_field_link l
    WHERE l.link_status = 'resolved_one_field_active'
)
SELECT
    pole_id,
    season_year,

    agr_operaciya_id,
    vid_raboty_id,
    oborudovanie_id,
    tip_zatrat,

    COUNT(*) AS waybill_lines,
    COUNT(DISTINCT put_list_doc_id) AS waybill_documents,
    COUNT(DISTINCT analitika_raskhodov_id) AS cost_analytics_count,
    COUNT(DISTINCT structure_code) AS structure_objects_count,

    MIN(work_date) AS first_work_date,
    MAX(work_date) AS last_work_date,

    SUM(COALESCE(gektarov, 0)) AS hectares,
    SUM(COALESCE(tonn, 0)) AS tons,
    SUM(COALESCE(kilometrov, 0)) AS kilometers,
    SUM(COALESCE(chasov, 0)) AS work_hours,
    SUM(COALESCE(mototochas, 0)) AS engine_hours,

    SUM(COALESCE(normativny_raskhod_gsm, 0)) AS normative_fuel_liters,
    SUM(COALESCE(labor_amount_rub, 0)) AS labor_amount_rub,

    CASE
        WHEN SUM(COALESCE(gektarov, 0)) <> 0
        THEN ROUND(
            SUM(COALESCE(normativny_raskhod_gsm, 0))
            / SUM(COALESCE(gektarov, 0)),
            4
        )
    END AS normative_fuel_liters_per_ha,

    CASE
        WHEN SUM(COALESCE(gektarov, 0)) <> 0
        THEN ROUND(
            SUM(COALESCE(labor_amount_rub, 0))
            / SUM(COALESCE(gektarov, 0)),
            2
        )
    END AS labor_rub_per_ha,

    COUNT(*) FILTER (
        WHERE crop_quality_status = 'crop_uuid'
    ) AS crop_uuid_lines,

    COUNT(*) FILTER (
        WHERE crop_quality_status = 'crop_missing'
    ) AS crop_missing_lines,

    COUNT(*) FILTER (
        WHERE crop_quality_status = 'crop_non_uuid_text'
    ) AS crop_non_uuid_text_lines,

    'normative_only' AS fuel_data_status,
    'resolved_one_field_active' AS field_link_status,
    now() AS _calculated_at
FROM source
GROUP BY
    pole_id,
    season_year,
    agr_operaciya_id,
    vid_raboty_id,
    oborudovanie_id,
    tip_zatrat;

COMMENT ON VIEW mart.v_field_season_operation_normative IS
    'Полевые работы из ПУЛ тракториста: поле/сезон/операция/техника. ГСМ нормативное, не фактическое; денежная стоимость топлива не рассчитывается.';

GRANT SELECT ON mart.v_field_season_operation_normative TO datalens_ro;
