-- ============================================================================
-- mart.fact_fuel_usage
-- Витрина расхода/выдачи топлива на основе документов
-- "АпкЗаправочныеВедомости".
--
-- Гранулярность: 1 строка = 1 строка табличной части ГСМ
-- (raw.r1c_zapravochnaya_vedomost_gsm), обогащенная атрибутами
-- шапки документа (raw.r1c_zapravochnaya_vedomost) и справочниками.
--
-- Важные особенности типов (проверено \d по всем таблицам 2026-09-07):
--   * doc.tehnika_id / doc.sotrudnik_id / gsm.marka_topliva_id и т.д. — uuid
--   * raw.r1c_equipment._id, raw.r1c_employees._id — text
--   * raw.r1c_org_units."Ref_Key", raw.r1c_warehouses."Ref_Key",
--     raw.r1c_nomenclature."Ref_Key" — text (сырые OData-таблицы без нормализации)
--   Поэтому все джойны на справочники идут через ::text.
--
-- Фильтр: только проведённые и не удалённые документы.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE VIEW mart.fact_fuel_usage AS
SELECT
    doc._id                                    AS doc_id,
    gsm.line_number,
    doc.doc_number,
    doc.doc_date,
    COALESCE(gsm.line_date, doc.doc_date::date) AS event_date,
    gsm.line_time                              AS event_time,

    -- техника: строка ГСМ может переопределять технику из шапки
    COALESCE(gsm.line_tehnika_id, doc.tehnika_id)          AS tehnika_id,
    eq.description                                          AS tehnika_name,
    eq.reg_number                                           AS tehnika_reg_number,
    eq.equipment_type                                       AS tehnika_type,

    -- сотрудник: аналогично, строка может переопределять сотрудника
    COALESCE(gsm.line_sotrudnik_id, doc.sotrudnik_id)      AS sotrudnik_id,
    emp.description                                         AS sotrudnik_name,

    gsm.marka_topliva_id,
    nom."Description"                                       AS marka_topliva_name,

    gsm.kolichestvo                                         AS kolichestvo_l,

    doc.organizaciya_id,
    org."Description"                                       AS organizaciya_name,

    doc.podrazdelenie_id,
    podr."Description"                                      AS podrazdelenie_name,

    doc.sklad_id,
    wh."Description"                                        AS sklad_name,

    doc.partner_id,
    doc.soglashenie_id,

    doc.vydacha_s_toplivozapravschika,
    doc.naemnoe_ts,
    doc.avtor_id,
    doc.kommentariy
FROM raw.r1c_zapravochnaya_vedomost_gsm gsm
JOIN raw.r1c_zapravochnaya_vedomost doc
    ON doc._id = gsm.doc_id
LEFT JOIN raw.r1c_equipment eq
    ON eq._id = COALESCE(gsm.line_tehnika_id, doc.tehnika_id)::text
LEFT JOIN raw.r1c_employees emp
    ON emp._id = COALESCE(gsm.line_sotrudnik_id, doc.sotrudnik_id)::text
LEFT JOIN raw.r1c_nomenclature nom
    ON nom."Ref_Key" = gsm.marka_topliva_id::text
LEFT JOIN raw.r1c_org_units org
    ON org."Ref_Key" = doc.organizaciya_id::text
LEFT JOIN raw.r1c_org_units podr
    ON podr."Ref_Key" = doc.podrazdelenie_id::text
LEFT JOIN raw.r1c_warehouses wh
    ON wh."Ref_Key" = doc.sklad_id::text
WHERE doc._posted = true
  AND (doc._deletionmark IS NULL OR doc._deletionmark = false);

COMMENT ON VIEW mart.fact_fuel_usage IS
    'Расход/выдача топлива по заправочным ведомостям. '
    'Источник: raw.r1c_zapravochnaya_vedomost(_gsm). '
    'Создано 2026-09-07.';

-- ----------------------------------------------------------------------------
-- Полезные агрегаты для DataLens: расход по дням/технике/марке.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.fact_fuel_usage_daily AS
SELECT
    event_date,
    tehnika_id,
    tehnika_name,
    tehnika_type,
    sotrudnik_id,
    sotrudnik_name,
    marka_topliva_id,
    marka_topliva_name,
    organizaciya_id,
    organizaciya_name,
    sklad_id,
    sklad_name,
    COUNT(*)               AS zapravok_count,
    SUM(kolichestvo_l)      AS kolichestvo_l_total
FROM mart.fact_fuel_usage
GROUP BY
    event_date, tehnika_id, tehnika_name, tehnika_type,
    sotrudnik_id, sotrudnik_name, marka_topliva_id, marka_topliva_name,
    organizaciya_id, organizaciya_name, sklad_id, sklad_name;

COMMENT ON VIEW mart.fact_fuel_usage_daily IS
    'Агрегат fact_fuel_usage по дням/технике/марке топлива для BI-графиков. Создано 2026-09-07.';

-- Доступ для BI-роли (если datalens_ro уже создана в 01_schemas.sql)
GRANT SELECT ON mart.fact_fuel_usage TO datalens_ro;
GRANT SELECT ON mart.fact_fuel_usage_daily TO datalens_ro;
