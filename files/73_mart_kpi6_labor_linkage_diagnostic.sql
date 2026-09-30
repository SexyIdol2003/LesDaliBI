-- KPI6: диагностика связи труда с объектом затрат и периодом контура.
-- Зерно: год × поле × дата работы × объект затрат.
-- Не меняет расчёт KPI6 и не выбирает сезонную площадь.

CREATE OR REPLACE VIEW mart.v_kpi6_labor_linkage_diagnostic AS
WITH work_events AS (
    SELECT
        EXTRACT(YEAR FROM w.work_date)::int AS calendar_year,
        w.pole_id::text AS pole_id,
        w.work_date::date AS work_date,
        w.analitika_raskhodov_id::text AS cost_object_key,
        COUNT(*) AS work_lines,
        SUM(COALESCE(w.labor_amount_rub, 0)) AS labor_rub
    FROM staging.stg_waybill_field_link w
    WHERE w.link_status = 'resolved_one_field_active'
      AND w.work_date >= DATE '2022-01-01'
      AND w.work_date < DATE '2027-01-01'
    GROUP BY 1, 2, 3, 4
)
SELECT
    w.calendar_year,
    w.pole_id,
    p.description AS field_name,
    w.work_date,
    w.cost_object_key,
    w.work_lines,
    w.labor_rub,
    area.economic_area_ha AS field_area_on_date_ha,
    m.exact_contour_matches,
    CASE
        WHEN m.exact_contour_matches = 1
            THEN 'EXACT'
        WHEN m.exact_contour_matches > 1
            THEN 'REVIEW_MULTIPLE_CONTOURS'
        WHEN area.economic_area_ha > 0
            THEN 'FIELD_ONLY'
        ELSE 'NO_AREA'
    END AS contour_allocation_status
FROM work_events w
LEFT JOIN raw.r1c_polya p
  ON p._id = w.pole_id
LEFT JOIN mart.v_field_economic_area_by_date area
  ON area.pole_id = w.pole_id
 AND area.snapshot_date = w.work_date
CROSS JOIN LATERAL (
    SELECT COUNT(*) AS exact_contour_matches
    FROM raw.r1c_polya_istoriya h
    WHERE h.pole_id = w.pole_id
      AND h.cost_object_key = w.cost_object_key
      AND COALESCE(h.eto_roditel, false) = false
      AND h.ploshad_obshaya > 0
      AND h.nachalo_perioda::date <= h.konec_perioda::date
      AND w.work_date BETWEEN
          h.nachalo_perioda::date AND h.konec_perioda::date
) m;

COMMENT ON VIEW mart.v_kpi6_labor_linkage_diagnostic IS
'Диагностика труда KPI6: EXACT — объект и период совпали; FIELD_ONLY — площадь поля есть, но объект и период не совпали; NO_AREA — площади на дату нет. Не является расчётом себестоимости на гектар.';

GRANT SELECT ON mart.v_kpi6_labor_linkage_diagnostic TO datalens_ro;
