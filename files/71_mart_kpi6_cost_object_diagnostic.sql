-- KPI6 diagnostic only: поле x календарный год x объект затрат.
-- Денежные меры частичные, ГСМ распределено расчётно.
-- Площадь контура не является автоматически площадью всего поля за сезон.

CREATE OR REPLACE VIEW mart.v_kpi6_cost_object_diagnostic AS
WITH labor AS (
    SELECT
        EXTRACT(YEAR FROM w.work_date)::int AS calendar_year,
        w.pole_id::text AS pole_id,
        w.analitika_raskhodov_id::text AS cost_object_key,
        COUNT(*) AS labor_rows,
        SUM(w.labor_amount_rub) AS labor_rub,
        COUNT(*) FILTER (WHERE w.labor_amount_rub IS NULL) AS labor_missing_amount_rows,
        COUNT(*) FILTER (
            WHERE h._id IS NOT NULL
              AND h.nachalo_perioda::date <= h.konec_perioda::date
              AND w.work_date::date NOT BETWEEN
                  h.nachalo_perioda::date AND h.konec_perioda::date
        ) AS labor_outside_contour_period_rows
    FROM staging.stg_waybill_field_link w
    LEFT JOIN raw.r1c_polya_istoriya h
      ON h.pole_id = w.pole_id
     AND h.cost_object_key = w.analitika_raskhodov_id::text
     AND COALESCE(h.eto_roditel, false) = false
     AND h.ploshad_obshaya > 0
    WHERE w.link_status = 'resolved_one_field_active'
      AND w.work_date >= DATE '2021-01-01'
      AND w.work_date < DATE '2027-01-01'
    GROUP BY 1, 2, 3
),
agro AS (
    SELECT
        a.season_year AS calendar_year,
        a.pole_id::text AS pole_id,
        a.apk_cost_object_key::text AS cost_object_key,
        COUNT(*) AS agro_rows,
        COUNT(*) FILTER (WHERE a.strict_cost_rub_no_vat IS NULL)
            AS agro_unpriced_rows,
        SUM(a.strict_cost_rub_no_vat) FILTER
            (WHERE a.agro_input_category = 'SEEDS') AS seeds_rub,
        SUM(a.strict_cost_rub_no_vat) FILTER
            (WHERE a.agro_input_category = 'FERTILIZERS') AS fertilizer_rub,
        SUM(a.strict_cost_rub_no_vat) FILTER
            (WHERE a.agro_input_category = 'CROP_PROTECTION') AS crop_protection_rub
    FROM mart.v_field_agro_input_cost_direct a
    WHERE a.pole_id IS NOT NULL
      AND a.season_year BETWEEN 2021 AND 2026
    GROUP BY 1, 2, 3
),
fuel AS (
    SELECT
        f.season_year AS calendar_year,
        f.pole_id::text AS pole_id,
        f.cost_object_key,
        COUNT(*) AS fuel_rows,
        SUM(f.allocated_object_liters) AS fuel_liters,
        SUM(f.allocated_object_rub_no_vat) AS fuel_rub_no_vat,
        COUNT(*) FILTER (
            WHERE f.allocated_object_liters > 0
              AND f.allocated_object_rub_no_vat IS NULL
        ) AS fuel_unpriced_rows
    FROM mart.v_field_fuel_cost_by_cost_object f
    GROUP BY 1, 2, 3
),
keys AS (
    SELECT calendar_year, pole_id, cost_object_key FROM labor
    UNION
    SELECT calendar_year, pole_id, cost_object_key FROM agro
    UNION
    SELECT calendar_year, pole_id, cost_object_key FROM fuel
),
components AS (
    SELECT
        k.calendar_year, k.pole_id, k.cost_object_key,
        l.labor_rows, l.labor_rub, l.labor_missing_amount_rows,
        l.labor_outside_contour_period_rows,
        a.agro_rows, a.agro_unpriced_rows,
        a.seeds_rub, a.fertilizer_rub, a.crop_protection_rub,
        f.fuel_rows, f.fuel_liters, f.fuel_rub_no_vat,
        f.fuel_unpriced_rows
    FROM keys k
    LEFT JOIN labor l USING (calendar_year, pole_id, cost_object_key)
    LEFT JOIN agro a USING (calendar_year, pole_id, cost_object_key)
    LEFT JOIN fuel f USING (calendar_year, pole_id, cost_object_key)
)
SELECT
    c.*,
    p.description AS field_name,
    s.apk_harvest_year AS object_harvest_year,
    h._id AS history_row_id,
    h.ploshad_obshaya AS contour_area_ha,
    h.nachalo_perioda::date AS contour_period_start_source,
    h.konec_perioda::date AS contour_period_end_source,
    (h.nachalo_perioda::date > h.konec_perioda::date)
        AS source_period_reversed,
    CASE
        WHEN h._id IS NULL THEN 'NO_CONTOUR_KEY'
        WHEN h.nachalo_perioda::date > h.konec_perioda::date
            THEN 'SOURCE_PERIOD_REVERSED'
        WHEN h.nachalo_perioda IS NULL OR h.konec_perioda IS NULL
            THEN 'PERIOD_NOT_VALIDATED'
        WHEN COALESCE(c.labor_outside_contour_period_rows, 0) > 0
            THEN 'LABOR_OUTSIDE_CONTOUR_PERIOD'
        WHEN c.fuel_rows IS NOT NULL
            THEN 'FUEL_MONTHLY_ALLOCATED_DATE_NOT_VERIFIED'
        ELSE 'KEY_MATCHED'
    END AS linkage_status,
    COALESCE(c.labor_rub, 0)
      + COALESCE(c.fuel_rub_no_vat, 0)
      + COALESCE(c.seeds_rub, 0)
      + COALESCE(c.fertilizer_rub, 0)
      + COALESCE(c.crop_protection_rub, 0)
      AS partial_known_cost_rub
FROM components c
LEFT JOIN raw.r1c_polya_istoriya h
  ON h.pole_id = c.pole_id
 AND h.cost_object_key = c.cost_object_key
 AND COALESCE(h.eto_roditel, false) = false
 AND h.ploshad_obshaya > 0
LEFT JOIN raw.r1c_polya p ON p._id = c.pole_id
LEFT JOIN raw.r1c_struktura_predpriyatiya s
  ON s.ref_key::text = c.cost_object_key;

COMMENT ON VIEW mart.v_kpi6_cost_object_diagnostic IS
'Диагностика прямых известных затрат по объекту/контуру: ФОТ + расчётное ГСМ + агровходы со строгой ценой. Итоговая сумма частичная; интервалы истории показываются как в источнике, без нормализации. Не полная фактическая себестоимость поля или сезона.';

GRANT SELECT ON mart.v_kpi6_cost_object_diagnostic TO datalens_ro;
