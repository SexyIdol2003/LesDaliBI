-- ============================================================================
-- 53_mart_field_day_operation_work_coverage.sql
--
-- Дневное покрытие полевых работ ПУЛ экономической площадью поля на дату.
--
-- Гранулярность:
--   поле × дата × агрооперация × вид работы × техника × тип затрат
--   × тип единицы объёма.
--
-- Назначение:
--   * разделить гектарные и почасовые производственные работы;
--   * связать подтверждённо полевые строки ПУЛ с экономической площадью на дату;
--   * показать зарплату и нормативный ГСМ без конвертации часов в гектары;
--   * дать DataLens прозрачные статусы покрытия и качества.
--
-- Ограничения:
--   * только 2026 год;
--   * в основной витрине только link_status = resolved_one_field_active;
--   * нормативный ГСМ не является фактическим списанием и не имеет денежной оценки;
--   * economic_area_ha нельзя суммировать между датами, операциями или строками;
--   * не является витриной полной себестоимости.
-- ============================================================================

DROP VIEW IF EXISTS mart.v_field_day_operation_work_coverage_quality;
DROP VIEW IF EXISTS mart.v_field_day_operation_work_coverage;

CREATE VIEW mart.v_field_day_operation_work_coverage AS
WITH source AS (
    SELECT
        l.put_list_line_id,
        l.put_list_doc_id,
        l.work_date::date AS work_date,
        EXTRACT(YEAR FROM l.work_date)::integer AS season_year,

        l.pole_id::text AS pole_id,
        a.field_name,

        l.agr_operaciya_id,
        l.vid_raboty_id,
        l.oborudovanie_id,
        e.description AS equipment_name,
        l.tip_zatrat,

        COALESCE(l.gektarov, 0) AS work_hectares,
        COALESCE(l.chasov, 0) AS work_hours,
        COALESCE(l.tonn, 0) AS work_tons,
        COALESCE(l.kilometrov, 0) AS work_kilometers,
        COALESCE(l.mototochas, 0) AS engine_hours,

        COALESCE(l.labor_amount_rub, 0) AS labor_amount_rub,
        COALESCE(l.normativny_raskhod_gsm, 0) AS normative_fuel_liters,

        l.link_status,
        l.link_source,

        a.economic_area_ha,
        a.active_contours_count,
        a.active_crops_count,
        a.economic_area_source,
        a.economic_area_method,
        a.economic_area_quality_status
    FROM staging.stg_waybill_field_link l
    JOIN mart.v_field_economic_area_by_date a
      ON a.pole_id = l.pole_id::text
     AND a.snapshot_date = l.work_date::date
    LEFT JOIN raw.r1c_equipment e
      ON e._id = l.oborudovanie_id
    WHERE l.work_date >= DATE '2026-01-01'
      AND l.work_date < DATE '2027-01-01'
      AND l.link_status = 'resolved_one_field_active'
),
classified AS (
    SELECT
        s.*,
        CASE
            WHEN s.work_hectares > 0 THEN 'HA'
            WHEN s.work_hours > 0 THEN 'HOURS'
            WHEN s.work_tons > 0 THEN 'TONS'
            WHEN s.work_kilometers > 0 THEN 'KILOMETERS'
            WHEN s.engine_hours > 0 THEN 'ENGINE_HOURS'
            ELSE 'NO_VOLUME'
        END AS work_volume_type,

        CASE
            WHEN s.work_hectares > 0 THEN s.work_hectares
            WHEN s.work_hours > 0 THEN s.work_hours
            WHEN s.work_tons > 0 THEN s.work_tons
            WHEN s.work_kilometers > 0 THEN s.work_kilometers
            WHEN s.engine_hours > 0 THEN s.engine_hours
            ELSE NULL
        END AS work_volume_value
    FROM source s
),
aggregated AS (
    SELECT
        work_date,
        season_year,
        pole_id,
        MAX(field_name) AS field_name,

        agr_operaciya_id,
        vid_raboty_id,
        oborudovanie_id,
        MAX(equipment_name) AS equipment_name,
        tip_zatrat,

        work_volume_type,

        COUNT(*) AS waybill_lines,
        COUNT(DISTINCT put_list_doc_id) AS waybill_documents,

        SUM(work_hectares) AS work_hectares,
        SUM(work_hours) AS work_hours,
        SUM(work_tons) AS work_tons,
        SUM(work_kilometers) AS work_kilometers,
        SUM(engine_hours) AS engine_hours,
        SUM(work_volume_value) AS work_volume_value,

        SUM(labor_amount_rub) AS labor_amount_rub,
        SUM(normative_fuel_liters) AS normative_fuel_liters,

        MAX(economic_area_ha) AS economic_area_ha,
        MAX(active_contours_count) AS active_contours_count,
        MAX(active_crops_count) AS active_crops_count,
        MAX(economic_area_source) AS economic_area_source,
        MAX(economic_area_method) AS economic_area_method,
        MAX(economic_area_quality_status) AS economic_area_quality_status,

        MAX(link_status) AS field_link_status,
        MAX(link_source) AS field_link_source
    FROM classified
    GROUP BY
        work_date,
        season_year,
        pole_id,
        agr_operaciya_id,
        vid_raboty_id,
        oborudovanie_id,
        tip_zatrat,
        work_volume_type
)
SELECT
    work_date,
    season_year,
    pole_id,
    field_name,

    agr_operaciya_id,
    vid_raboty_id,
    oborudovanie_id,
    equipment_name,
    tip_zatrat,

    work_volume_type,

    waybill_lines,
    waybill_documents,

    ROUND(work_hectares, 4) AS work_hectares,
    ROUND(work_hours, 4) AS work_hours,
    ROUND(work_tons, 4) AS work_tons,
    ROUND(work_kilometers, 4) AS work_kilometers,
    ROUND(engine_hours, 4) AS engine_hours,
    ROUND(work_volume_value, 4) AS work_volume_value,

    ROUND(labor_amount_rub, 2) AS labor_amount_rub,
    ROUND(normative_fuel_liters, 4) AS normative_fuel_liters,

    ROUND(economic_area_ha, 4) AS economic_area_ha,
    active_contours_count,
    active_crops_count,
    economic_area_source,
    economic_area_method,
    economic_area_quality_status,

    ROUND(
        labor_amount_rub / NULLIF(work_hectares, 0),
        4
    ) AS labor_rub_per_work_ha,

    ROUND(
        normative_fuel_liters / NULLIF(work_hectares, 0),
        4
    ) AS normative_fuel_liters_per_work_ha,

    ROUND(
        labor_amount_rub / NULLIF(work_hours, 0),
        4
    ) AS labor_rub_per_work_hour,

    ROUND(
        normative_fuel_liters / NULLIF(work_hours, 0),
        4
    ) AS normative_fuel_liters_per_work_hour,

    CASE
        WHEN work_volume_type = 'HA'
            THEN 'OK_FIELD_LINKED_HA'
        WHEN work_volume_type = 'HOURS'
            THEN 'OK_FIELD_LINKED_HOURS'
        WHEN work_volume_type IN ('TONS', 'KILOMETERS', 'ENGINE_HOURS')
            THEN 'FIELD_LINKED_OTHER_VOLUME'
        ELSE 'FIELD_LINKED_NO_VOLUME'
    END AS work_measure_quality_status,

    'RESOLVED_ONE_FIELD_ACTIVE_2026'::text AS coverage_scope,

    'NORMATIVE_ONLY'::text AS fuel_data_status,

    now() AS calculated_at
FROM aggregated;

COMMENT ON VIEW mart.v_field_day_operation_work_coverage IS
    'Полевые работы ПУЛ 2026 с однозначной привязкой к полю и экономической площадью на дату. Гектарные и почасовые работы разделены; нормативный ГСМ не является фактом.';

CREATE VIEW mart.v_field_day_operation_work_coverage_quality AS
WITH all_waybill_2026 AS (
    SELECT
        l.put_list_line_id,
        l.put_list_doc_id,
        l.work_date::date AS work_date,
        l.link_status,
        COALESCE(l.gektarov, 0) AS work_hectares,
        COALESCE(l.chasov, 0) AS work_hours,
        COALESCE(l.labor_amount_rub, 0) AS labor_amount_rub,
        COALESCE(l.normativny_raskhod_gsm, 0) AS normative_fuel_liters
    FROM staging.stg_waybill_field_link l
    WHERE l.work_date >= DATE '2026-01-01'
      AND l.work_date < DATE '2027-01-01'
),
quality_by_link AS (
    SELECT
        COALESCE(link_status, 'NULL') AS field_link_status,

        COUNT(*) AS waybill_lines,
        COUNT(DISTINCT put_list_doc_id) AS waybill_documents,

        SUM(work_hectares) AS work_hectares,
        SUM(work_hours) AS work_hours,
        SUM(labor_amount_rub) AS labor_amount_rub,
        SUM(normative_fuel_liters) AS normative_fuel_liters,

        COUNT(*) FILTER (
            WHERE work_hectares > 0
        ) AS ha_lines,

        COUNT(*) FILTER (
            WHERE work_hectares = 0
              AND work_hours > 0
        ) AS hour_lines,

        COUNT(*) FILTER (
            WHERE work_hectares = 0
              AND work_hours = 0
        ) AS other_or_no_volume_lines
    FROM all_waybill_2026
    GROUP BY COALESCE(link_status, 'NULL')
)
SELECT
    field_link_status,

    waybill_lines,
    waybill_documents,

    ROUND(work_hectares, 4) AS work_hectares,
    ROUND(work_hours, 4) AS work_hours,
    ROUND(labor_amount_rub, 2) AS labor_amount_rub,
    ROUND(normative_fuel_liters, 4) AS normative_fuel_liters,

    ha_lines,
    hour_lines,
    other_or_no_volume_lines,

    ROUND(
        100.0 * waybill_lines
        / NULLIF(SUM(waybill_lines) OVER (), 0),
        2
    ) AS lines_share_pct,

    ROUND(
        100.0 * labor_amount_rub
        / NULLIF(SUM(labor_amount_rub) OVER (), 0),
        2
    ) AS labor_share_pct,

    ROUND(
        100.0 * normative_fuel_liters
        / NULLIF(SUM(normative_fuel_liters) OVER (), 0),
        2
    ) AS normative_fuel_share_pct,

    CASE
        WHEN field_link_status = 'resolved_one_field_active'
            THEN 'INCLUDED_IN_MAIN_VIEW'
        ELSE 'EXCLUDED_NO_UNAMBIGUOUS_FIELD_LINK'
    END AS inclusion_status,

    '2026_ONLY'::text AS coverage_scope,

    now() AS calculated_at
FROM quality_by_link;

COMMENT ON VIEW mart.v_field_day_operation_work_coverage_quality IS
    'QA покрытия ПУЛ 2026: статус привязки к полю, гектарные/почасовые строки, зарплата и нормативный ГСМ.';

GRANT SELECT ON mart.v_field_day_operation_work_coverage TO datalens_ro;
GRANT SELECT ON mart.v_field_day_operation_work_coverage_quality TO datalens_ro;
