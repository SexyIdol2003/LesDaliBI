INSERT INTO staging.map_apk_cost_object_to_field (
    apk_cost_object_key,
    field_sk,
    mapping_source,
    mapping_note,
    valid_from,
    valid_to,
    is_active
)
SELECT
    s.ref_key AS apk_cost_object_key,
    d.field_sk,
    'raw.r1c_struktura_predpriyatiya.apk_pole_key' AS mapping_source,
    'Автоматический маппинг через Структуру предприятия: '
        || COALESCE(s.description, '') AS mapping_note,
    DATE '2000-01-01' AS valid_from,
    DATE '9999-12-31' AS valid_to,
    true AS is_active
FROM (
    SELECT DISTINCT
        apk_cost_object_key
    FROM mart.v_agro_input_cost_per_ha_2026
    WHERE apk_cost_object_key IS NOT NULL
) u
JOIN raw.r1c_struktura_predpriyatiya s
    ON s.ref_key::text = u.apk_cost_object_key
   AND COALESCE(s.deletion_mark, false) = false
JOIN mart.dim_field d
    ON d.field_code_1c = s.apk_pole_key::text
   AND d.is_current = true
ON CONFLICT (apk_cost_object_key) DO UPDATE
SET
    field_sk = EXCLUDED.field_sk,
    mapping_source = EXCLUDED.mapping_source,
    mapping_note = EXCLUDED.mapping_note,
    valid_from = EXCLUDED.valid_from,
    valid_to = EXCLUDED.valid_to,
    is_active = EXCLUDED.is_active,
    updated_at = now();

CREATE OR REPLACE VIEW mart.v_agro_input_cost_per_field_season AS
WITH costs_by_field AS (
    SELECT
        EXTRACT(YEAR FROM a.document_date)::integer AS season,
        m.field_sk,
        a.agro_category_1c AS category,
        COUNT(*) AS material_lines,
        SUM(a.material_cost_no_vat) AS total_cost_rub_no_vat
    FROM mart.v_agro_input_cost_per_ha_2026 a
    JOIN staging.map_apk_cost_object_to_field m
        ON m.apk_cost_object_key::text = a.apk_cost_object_key
       AND a.document_date BETWEEN m.valid_from AND m.valid_to
       AND m.is_active = true
    WHERE a.data_quality_status = 'OK'
    GROUP BY
        EXTRACT(YEAR FROM a.document_date)::integer,
        m.field_sk,
        a.agro_category_1c
),
field_area_by_season AS (
    SELECT
        h.god_urozhaya AS season,
        d.field_sk,
        MAX(h.field_name) AS field_name,
        MAX(h.area_ha) AS field_area_ha
    FROM mart.v_field_area_by_season h
    JOIN mart.dim_field d
        ON d.field_code_1c = h.pole_id
       AND d.is_current = true
    WHERE h.area_ha IS NOT NULL
      AND h.area_ha > 0
    GROUP BY
        h.god_urozhaya,
        d.field_sk
)
SELECT
    c.season,
    c.field_sk,
    fa.field_name,
    c.category,
    c.material_lines,
    ROUND(c.total_cost_rub_no_vat, 2) AS total_cost_rub_no_vat,
    ROUND(fa.field_area_ha, 2) AS field_area_ha,
    ROUND(
        c.total_cost_rub_no_vat
        / NULLIF(fa.field_area_ha, 0),
        2
    ) AS cost_rub_per_field_ha,
    'OK: только подтвержденные цены'::text AS data_quality_status
FROM costs_by_field c
JOIN field_area_by_season fa
    ON fa.season = c.season
   AND fa.field_sk = c.field_sk;

GRANT SELECT ON mart.v_agro_input_cost_per_field_season TO datalens_ro;
