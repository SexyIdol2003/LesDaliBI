-- ============================================================================
-- 55_mart_field_day_operation_labor_cost.sql
--
-- Фактические трудовые затраты по работам на поле.
--
-- Гранулярность:
--   дата работы × поле × агрооперация × вид работы × техника × тип затрат.
--
-- Источник труда:
--   staging.stg_waybill_field_link.labor_amount_rub.
--
-- Область включения:
--   Только строки, где поле однозначно разрешено как активное:
--   link_status = 'resolved_one_field_active'.
--
-- Экономическая площадь:
--   mart.v_field_economic_area_by_date по ключу pole_id + work_date.
--   Площадь — знаменатель на дату, а не объём выполненной работы.
--
-- Важно:
--   * labor_amount_rub является фактической суммой труда из путевых листов.
--   * Витрина не представляет весь труд предприятия: исключены строки без
--     однозначно разрешённого активного поля.
--   * economic_area_ha нельзя суммировать между датами, операциями или
--     строками витрины.
--   * labor_rub_per_economic_ha не является сезонной себестоимостью ₽/га:
--     поле может иметь несколько работ в один день и в сезоне.
--   * При отсутствии площади на дату ₽/экономический га остаётся NULL,
--     а строка сохраняется со статусом NO_ECONOMIC_AREA_ON_WORK_DATE.
-- ============================================================================

CREATE OR REPLACE VIEW mart.v_field_day_operation_labor_cost AS
WITH source AS (
    SELECT
        l.put_list_line_id,
        l.put_list_doc_id,
        l.work_date,
        EXTRACT(YEAR FROM l.work_date)::integer AS season_year,
        l.pole_id,
        a.field_name,

        l.agr_operaciya_id,
        l.vid_raboty_id,
        l.oborudovanie_id,
        e.description AS equipment_name,
        l.tip_zatrat,

        COALESCE(l.gektarov, 0)::numeric AS work_hectares,
        COALESCE(l.chasov, 0)::numeric AS work_hours,
        COALESCE(l.tonn, 0)::numeric AS work_tons,
        COALESCE(l.kilometrov, 0)::numeric AS work_kilometers,
        COALESCE(l.mototochas, 0)::numeric AS engine_hours,
        COALESCE(l.labor_amount_rub, 0)::numeric AS labor_amount_rub,

        l.link_status,
        l.link_source,

        a.economic_area_ha,
        a.active_contours_count,
        a.active_crops_count,
        a.economic_area_source,
        a.economic_area_method,
        a.economic_area_quality_status
    FROM staging.stg_waybill_field_link l
    LEFT JOIN mart.v_field_economic_area_by_date a
      ON a.pole_id = l.pole_id
     AND a.snapshot_date = l.work_date
    LEFT JOIN raw.r1c_equipment e
      ON e._id = l.oborudovanie_id
    WHERE l.work_date >= DATE '2021-01-01'
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
            ELSE NULL::numeric
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
        labor_amount_rub / NULLIF(work_hours, 0),
        4
    ) AS labor_rub_per_work_hour,

    ROUND(
        labor_amount_rub / NULLIF(economic_area_ha, 0),
        4
    ) AS labor_rub_per_economic_ha,

    CASE
        WHEN economic_area_ha IS NULL OR economic_area_ha <= 0
            THEN 'NO_ECONOMIC_AREA_ON_WORK_DATE'
        ELSE 'OK_ECONOMIC_AREA_ON_WORK_DATE'
    END AS economic_area_coverage_status,

    CASE
        WHEN labor_amount_rub > 0 AND work_hectares > 0
            THEN 'OK_LABOR_AND_WORK_HA'
        WHEN labor_amount_rub > 0
            THEN 'LABOR_WITHOUT_WORK_HA'
        WHEN labor_amount_rub = 0 AND work_hectares > 0
            THEN 'WORK_HA_WITHOUT_LABOR'
        ELSE 'NO_LABOR_AND_NO_WORK_HA'
    END AS labor_quality_status,

    'ACTUAL_LABOR_FROM_WAYBILL'::text AS labor_cost_source,
    now() AS calculated_at
FROM aggregated;

COMMENT ON VIEW mart.v_field_day_operation_labor_cost IS
    'Фактические трудовые затраты по работам на однозначно связанных активных полях за 2021-2026. Площадь присоединяется по полю и дате работы; NULL не заменяется. Не является полной себестоимостью и не включает строки без разрешённого поля.';

GRANT SELECT ON mart.v_field_day_operation_labor_cost TO datalens_ro;
