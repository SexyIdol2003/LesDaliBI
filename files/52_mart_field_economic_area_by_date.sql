-- ============================================================================
-- 52_mart_field_economic_area_by_date.sql
--
-- Экономическая площадь поля на календарную дату по истории полей 1С.
--
-- Гранулярность:
--   поле × дата.
--
-- Методика:
--   1. Используются только дочерние строки ИсторииПоля: eto_roditel = false.
--   2. В расчёт включаются строки с положительной общей площадью:
--      ploshad_obshaya > 0.
--   3. Для 2021-2025 используются архивные строки ИсторииПоля:
--      ne_ispolzuetsya = true.
--   4. Для 2026 используются текущие активные строки ИсторииПоля:
--      ne_ispolzuetsya = false.
--   5. Площадь на дату = сумма площадей всех контуров, активных на дату.
--   6. Непокрытые даты не получают искусственную площадь и отсутствуют
--      в витрине; в зависимых слоях должны оставаться NULL.
--
-- Доказательная база метода:
--   * На датах работ 2021-2025 встречаются только архивные строки истории.
--   * На датах работ 2026 встречаются только активные строки истории.
--   * На датах работ отсутствуют смешанные ключи active + archived.
--
-- Не является витриной полной себестоимости. Не содержит ГСМ, ремонтов,
-- зарплаты, агровходов или итогового ₽/га.
-- ============================================================================

DROP VIEW IF EXISTS mart.v_field_economic_area_by_date_quality;

CREATE OR REPLACE VIEW mart.v_field_economic_area_by_date AS
WITH history_by_period AS (
    SELECT
        h._id AS history_row_id,
        h.pole_id::text AS pole_id,
        p.description AS field_name,
        h.kultura_id::text AS crop_id,
        c.description AS crop_name,
        c.crop_kind,
        h.ploshad_obshaya AS contour_area_ha,
        LEAST(h.nachalo_perioda::date, h.konec_perioda::date) AS period_start,
        GREATEST(h.nachalo_perioda::date, h.konec_perioda::date) AS period_end,
        COALESCE(h.ne_ispolzuetsya, false) AS is_archived_history_row
    FROM raw.r1c_polya_istoriya h
    JOIN raw.r1c_polya p
      ON p._id = h.pole_id
    LEFT JOIN raw.r1c_crops c
      ON c._id = h.kultura_id
    WHERE COALESCE(h.eto_roditel, false) = false
      AND h.ploshad_obshaya > 0
      AND h.nachalo_perioda >= DATE '0002-01-01'
      AND h.konec_perioda >= DATE '0002-01-01'
      AND LEAST(h.nachalo_perioda::date, h.konec_perioda::date)
            < DATE '2027-01-01'
      AND GREATEST(h.nachalo_perioda::date, h.konec_perioda::date)
            >= DATE '2021-01-01'
),
field_dates AS (
    SELECT DISTINCT
        h.pole_id,
        h.field_name,
        d.snapshot_ts::date AS snapshot_date
    FROM history_by_period h
    CROSS JOIN LATERAL generate_series(
        GREATEST(h.period_start, DATE '2021-01-01'),
        LEAST(h.period_end, DATE '2026-12-31'),
        INTERVAL '1 day'
    ) AS d(snapshot_ts)
    WHERE (
            (
                EXTRACT(YEAR FROM d.snapshot_ts)::integer BETWEEN 2021 AND 2025
                AND h.is_archived_history_row = true
            )
         OR (
                EXTRACT(YEAR FROM d.snapshot_ts)::integer = 2026
                AND h.is_archived_history_row = false
            )
         )
),
area_by_date AS (
    SELECT
        fd.snapshot_date,
        EXTRACT(YEAR FROM fd.snapshot_date)::integer AS season,
        fd.pole_id,
        fd.field_name,

        COUNT(h.history_row_id) AS active_contours_count,
        COUNT(DISTINCT h.crop_id) AS active_crops_count,

        ROUND(SUM(h.contour_area_ha), 4) AS economic_area_ha,
        ROUND(MAX(h.contour_area_ha), 4) AS max_single_contour_area_ha,

        MIN(h.period_start) AS min_active_period_start,
        MAX(h.period_end) AS max_active_period_end,

        STRING_AGG(
            COALESCE(h.crop_name, h.crop_id, 'Культура не указана')
            || ': '
            || ROUND(h.contour_area_ha, 2)::text
            || ' га',
            ' | '
            ORDER BY h.contour_area_ha DESC, h.crop_name
        ) AS active_crops_and_areas,

        ARRAY_AGG(
            h.history_row_id
            ORDER BY h.contour_area_ha DESC, h.history_row_id
        ) AS history_row_ids,

        CASE
            WHEN EXTRACT(YEAR FROM fd.snapshot_date)::integer BETWEEN 2021 AND 2025
                THEN 'ARCHIVED_ACTIVE_CONTOURS_SUM_ON_DATE'
            WHEN EXTRACT(YEAR FROM fd.snapshot_date)::integer = 2026
                THEN 'ACTIVE_CONTOURS_SUM_ON_DATE'
            ELSE 'CHECK_AREA_METHOD'
        END AS economic_area_method,

        CASE
            WHEN EXTRACT(YEAR FROM fd.snapshot_date)::integer BETWEEN 2021 AND 2025
                THEN 'ARCHIVED_HISTORY_FIELD_CONTOURS'
            WHEN EXTRACT(YEAR FROM fd.snapshot_date)::integer = 2026
                THEN 'ACTIVE_HISTORY_FIELD_CONTOURS'
            ELSE 'CHECK_AREA_SOURCE'
        END AS economic_area_source
    FROM field_dates fd
    JOIN history_by_period h
      ON h.pole_id = fd.pole_id
     AND fd.snapshot_date BETWEEN h.period_start AND h.period_end
     AND (
            (
                EXTRACT(YEAR FROM fd.snapshot_date)::integer BETWEEN 2021 AND 2025
                AND h.is_archived_history_row = true
            )
         OR (
                EXTRACT(YEAR FROM fd.snapshot_date)::integer = 2026
                AND h.is_archived_history_row = false
            )
         )
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

    economic_area_source,
    economic_area_method,

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
    'Экономическая площадь поля на дату за 2021-2026. 2021-2025: сумма архивных контуров ИсторииПоля, активных на дату. 2026: сумма текущих активных контуров ИсторииПоля, активных на дату. Непокрытые даты не заполняются искусственно.';

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
    'QA-витрина экономической площади поля на дату за 2021-2026. Для 2021-2025 использует архивные контуры, для 2026 — активные контуры.';

GRANT SELECT ON mart.v_field_economic_area_by_date TO datalens_ro;
GRANT SELECT ON mart.v_field_economic_area_by_date_quality TO datalens_ro;
