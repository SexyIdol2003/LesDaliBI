-- Read-only diagnostic: document calendar year vs linked cost object's harvest year.
-- A date inside the source period is a candidate, not an accounting reallocation.
-- Do not add these amounts to KPI: they are already present in calendar-year costs.
-- Grain: (document_id, line_number). Multiple joined rows are quarantined,
-- rather than summed or silently selected with DISTINCT ON.

WITH object_period AS (
    SELECT DISTINCT
        calendar_year,
        pole_id,
        cost_object_key,
        object_harvest_year,
        contour_period_start_source,
        contour_period_end_source
    FROM mart.v_kpi6_cost_object_diagnostic
),
matched AS (
    SELECT
        a.document_date,
        a.doc_number,
        a.document_id,
        a.line_number,
        a.field_name,
        a.pole_id,
        a.apk_cost_object_key,
        a.nomenklatura_name,
        a.agro_input_category,
        a.season_year AS document_year,
        o.object_harvest_year AS harvest_year,
        o.contour_period_start_source,
        o.contour_period_end_source,
        a.strict_cost_rub_no_vat,
        CASE
            WHEN o.contour_period_start_source IS NULL
              OR o.contour_period_end_source IS NULL
                THEN 'REVIEW_MISSING_PERIOD'
            WHEN o.contour_period_start_source > o.contour_period_end_source
                THEN 'REVIEW_REVERSED_PERIOD'
            WHEN a.document_date BETWEEN o.contour_period_start_source
                                     AND o.contour_period_end_source
                THEN 'CANDIDATE_IN_PERIOD'
            ELSE 'REVIEW_OUTSIDE_PERIOD'
        END AS date_status
    FROM mart.v_field_agro_input_cost_direct a
    JOIN object_period o
      ON o.calendar_year = a.season_year
     AND o.pole_id = a.pole_id
     AND o.cost_object_key = a.apk_cost_object_key
    WHERE a.strict_cost_rub_no_vat IS NOT NULL
      AND o.object_harvest_year::text IS DISTINCT FROM a.season_year::text
),
per_line AS (
    SELECT
        document_id,
        line_number,
        COUNT(*) AS match_rows,
        MAX(document_date) AS document_date,
        MAX(doc_number) AS doc_number,
        MAX(field_name) AS field_name,
        MAX(pole_id::text) AS pole_id,
        MAX(apk_cost_object_key::text) AS apk_cost_object_key,
        MAX(nomenklatura_name) AS nomenklatura_name,
        MAX(agro_input_category) AS agro_input_category,
        MAX(document_year) AS document_year,
        MAX(harvest_year) AS harvest_year,
        MAX(contour_period_start_source) AS contour_period_start_source,
        MAX(contour_period_end_source) AS contour_period_end_source,
        MAX(strict_cost_rub_no_vat) AS strict_cost_rub_no_vat,
        CASE
            WHEN COUNT(*) > 1 THEN 'REVIEW_DUPLICATE_JOIN'
            ELSE MIN(date_status)
        END AS diagnostic_status
    FROM matched
    GROUP BY document_id, line_number
)
SELECT
    document_date, doc_number, document_id, line_number,
    field_name, pole_id, apk_cost_object_key,
    nomenklatura_name, agro_input_category,
    document_year, harvest_year,
    contour_period_start_source, contour_period_end_source,
    match_rows,
    CASE WHEN match_rows = 1 THEN strict_cost_rub_no_vat END
        AS unambiguous_rub,
    diagnostic_status
FROM per_line
ORDER BY document_date, document_id, line_number;

-- Aggregate independently from the same CTE definition, if needed:
-- Keep rows with match_rows > 1 in a separate REVIEW_DUPLICATE_JOIN group;
-- never include their amounts in the unambiguous total.
