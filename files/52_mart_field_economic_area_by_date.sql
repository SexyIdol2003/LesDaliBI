-- ============================================================================
-- 52_mart_field_economic_area_by_date.sql
--
-- Экономическая площадь поля на календарную дату по истории полей 1С.
--
-- Гранулярность:
--   поле × дата
--
-- Методика:
--   1. Используются только дочерние строки ИсторииПоля:
--      eto_roditel = false.
--   2. Исключаются неиспользуемые строки:
--      ne_ispolzuetsya = false.
--   3. В расчёт включаются строки с положительной общей площадью:
--      ploshad_obshaya > 0.
--   4. Площадь на дату = сумма площадей всех контуров, активных на дату.
--   5. Сезон определяется из календарной даты, а не god_urozhaya:
--      в детальных строках истории полей god_urozhaya может быть равен 0.
--
-- Ограничение версии:
--   Витрина намеренно ограничена 2026 годом. Для 2021-2025 требуется
--   отдельная проверка исторической связности площади с затратами.
--
-- Не является витриной полной себестоимости. Не содержит ГСМ, ремонтов,
-- зарплаты, агровходов или итогового ₽/га.
-- ============================================================================

DROP VIEW IF EXISTS mart.v_field_economic_area_by_date_quality;
DROP VIEW IF EXISTS mart.v_field_economic_area_by_date;

CREATE VIEW mart.v_field_economic_area_by_date AS
WITH calendar AS (
    SELECT generate_series(
        DATE '2026-01-01',
        DATE '2026-12-31',
        INTERVAL '1 day'
    )::date AS snapshot_date
),
active_history AS (
    SELECT
        h._id AS history_row_id,
        h.pole_id::text AS pole_id,
        p.description AS field_name,
        h.kultura_id::text AS crop_id,
        c.description AS crop_name,
        c.crop_kind,
        h.ploshad_obshaya AS contour_area_ha,
        h.nachalo_perioda::date AS period_start,
        h.konec_perioda::date AS period_end
    FROM raw.r1c_polya_istoriya h
    JOIN raw.r1c_polya p
      ON p._id = h.pole_id
    LEFT JOIN raw.r1c_crops c
      ON c._id = h.kultura_id
    WHERE COALESCE(h.eto_roditel, false) = false
      AND COALESCE(h.ne_ispolzuetsya, false) = false
      AND h.ploshad_obshaya > 0
      AND h.nachalo_perioda >= DATE '0002-01-01'
      AND h.konec_perioda >= DATE '0002-01-01'
      AND h.nachalo_perioda < DATE '2027-01-01'
      AND h.konec_perioda >= DATE '2026-01-01'
),
field_dates AS (
    SELECT DISTINCT
        ah.pole_id,
        ah.field_name,
        c.snapshot_date
    FROM active_history ah
    JOIN calendar c
      ON c.snapshot_date BETWEEN ah.period_start AND ah.period_end
),
area_by_date AS (
    SELECT
        fd.snapshot_date,
        EXTRACT(YEAR FROM fd.snapshot_date)::integer AS season,
        fd.pole_id,
        fd.field_name,

        COUNT(ah.history_row_id) AS active_contours_count,
        COUNT(DISTINCT ah.crop_id) AS active_crops_count,

        ROUND(SUM(ah.contour_area_ha), 4) AS economic_area_ha,
        ROUND(MAX(ah.contour_area_ha), 4) AS max_single_contour_area_ha,

        MIN(ah.period_start) AS min_active_period_start,
        MAX(ah.period_end) AS max_active_period_end,

        STRING_AGG(
            COALESCE(ah.crop_name, ah.crop_id, 'Культура не указана')
            || ': '
            || ROUND(ah.contour_area_ha, 2)::text
            || ' га',
            ' | '
            ORDER BY ah.contour_area_ha DESC, ah.crop_name
        ) AS active_crops_and_areas,

        ARRAY_AGG(
            ah.history_row_id
            ORDER BY ah.contour_area_ha DESC, ah.history_row_id
        ) AS history_row_ids
    FROM field_dates fd
    JOIN active_history ah
      ON ah.pole_id = fd.pole_id
     AND fd.snapshot_date BETWEEN ah.period_start AND ah.period_end
    GROUP BY
        fd.snapshot_date,
        fd.pole_id,
        fd.field_name
)
SELECT
    snapshot_date,
    season,
    pole_id,
    field_name,

    active_contours_count,
    active_crops_count,
    economic_area_ha,
    max_single_contour_area_ha,

    min_active_period_start,
    max_active_period_end,
    active_crops_and_areas,
    history_row_ids,

    'raw.r1c_polya_istoriya.ploshad_obshaya'::text
        AS economic_area_source,

    'ACTIVE_CONTOURS_SUM_ON_DATE'::text
        AS economic_area_method,

    CASE
        WHEN economic_area_ha IS NULL OR economic_area_ha <= 0
            THEN 'NO_ACTIVE_AREA'
        WHEN active_contours_count = 1
            THEN 'OK_SINGLE_ACTIVE_CONTOUR'
        WHEN active_contours_count > 1
            THEN 'OK_MULTIPLE_ACTIVE_CONTOURS_SUMMED'
        ELSE 'CHECK_AREA'
    END AS economic_area_quality_status,

    now() AS calculated_at
FROM area_by_date;

COMMENT ON VIEW mart.v_field_economic_area_by_date IS
    'Экономическая площадь поля на дату за 2026 год. Рассчитывается как сумма активных дочерних контуров raw.r1c_polya_istoriya. Не является витриной полной себестоимости.';

CREATE VIEW mart.v_field_economic_area_by_date_quality AS
SELECT
    snapshot_date,
    season,
    pole_id,
    field_name,

    active_contours_count,
    active_crops_count,
    economic_area_ha,
    max_single_contour_area_ha,

    min_active_period_start,
    max_active_period_end,

    economic_area_source,
    economic_area_method,
    economic_area_quality_status,

    CASE
        WHEN active_contours_count > 1 THEN true
        ELSE false
    END AS is_multi_contour_area,

    CASE
        WHEN active_crops_count > 1 THEN true
        ELSE false
    END AS is_multi_crop_area,

    CASE
        WHEN economic_area_ha > max_single_contour_area_ha THEN true
        ELSE false
    END AS sum_differs_from_max_contour,

    calculated_at
FROM mart.v_field_economic_area_by_date;

COMMENT ON VIEW mart.v_field_economic_area_by_date_quality IS
    'QA-витрина экономической площади поля на дату за 2026 год: число контуров, число культур, метод и статус качества.';

GRANT SELECT ON mart.v_field_economic_area_by_date TO datalens_ro;
GRANT SELECT ON mart.v_field_economic_area_by_date_quality TO datalens_ro;
