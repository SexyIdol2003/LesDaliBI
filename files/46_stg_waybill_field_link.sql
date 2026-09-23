-- 46_stg_waybill_field_link.sql
-- Мост строк ПУЛ тракториста к полям через аналитику расходов.
--
-- Логика:
--   raw.r1c_putevoy_list_lines.analitika_raskhodov_id
--     -> raw.r1c_struktura_predpriyatiya.ref_key
--     -> raw.r1c_struktura_predpriyatiya.apk_pole_key
--     -> raw.r1c_polya_istoriya (год урожая и культура).
--
-- ВАЖНО:
--   normativny_raskhod_gsm — нормативный, а не фактический расход ГСМ.
--   Витрина не предназначена для денежной себестоимости ГСМ,
--   пока не найден источник фактического списания и метод оценки.

CREATE OR REPLACE VIEW staging.stg_waybill_field_link AS
WITH structure_base AS (
    SELECT
        s.ref_key::text AS analitika_raskhodov_id,
        s.code AS structure_code,
        s.description AS structure_description,
        s.apk_pole_key::text AS pole_id,
        COALESCE(s.deletion_mark, false) AS structure_deletion_mark,

        COALESCE(
            s.apk_harvest_year,
            CASE
                WHEN s.description ~ '[0-9]{4}[[:space:]]*г\.?'
                THEN substring(
                    s.description
                    FROM '([0-9]{4})[[:space:]]*г\.?'
                )::int
            END
        ) AS season_year_from_structure
    FROM raw.r1c_struktura_predpriyatiya s
    WHERE s.apk_pole_key IS NOT NULL
      AND s.apk_pole_key <> '00000000-0000-0000-0000-000000000000'::uuid
),
structure_cardinality AS (
    SELECT
        analitika_raskhodov_id,
        COUNT(DISTINCT pole_id) AS pole_count_per_analytic
    FROM structure_base
    GROUP BY analitika_raskhodov_id
),
structure_map AS (
    SELECT
        sb.*,
        sc.pole_count_per_analytic
    FROM structure_base sb
    JOIN structure_cardinality sc
      ON sc.analitika_raskhodov_id = sb.analitika_raskhodov_id
),
field_history_ranked AS (
    SELECT
        h.pole_id::text AS pole_id,
        h.god_urozhaya::int AS season_year,
        h.kultura_id::text AS crop_id,
        ROW_NUMBER() OVER (
            PARTITION BY h.pole_id, h.god_urozhaya
            ORDER BY h.kultura_id NULLS LAST
        ) AS rn
    FROM raw.r1c_polya_istoriya h
    WHERE h.pole_id IS NOT NULL
),
field_history AS (
    SELECT
        pole_id,
        season_year,
        crop_id
    FROM field_history_ranked
    WHERE rn = 1
),
base AS (
    SELECT
        p._id AS put_list_line_id,
        p.doc_id AS put_list_doc_id,
        p.line_number,
        p.den_raboty AS work_date,

        p.analitika_raskhodov_id::text AS analitika_raskhodov_id,
        p.klyuch_svyazi::text AS klyuch_svyazi,
        p.agr_operaciya_id::text AS agr_operaciya_id,
        p.vid_raboty_id::text AS vid_raboty_id,
        p.oborudovanie_id::text AS oborudovanie_id,

        p.gektarov,
        p.tonn,
        p.kilometrov,
        p.chasov,
        p.mototochas,
        p.normativny_raskhod_gsm,
        p.itogo_zp AS labor_amount_rub,
        p.tip_zatrat,

        s.structure_code,
        s.structure_description,
        s.pole_id,
        s.structure_deletion_mark,
        s.pole_count_per_analytic,
        s.season_year_from_structure,

        h.season_year,
        h.crop_id,

        CASE
            WHEN p.analitika_raskhodov_id IS NULL
                THEN 'missing_analytic'
            WHEN s.analitika_raskhodov_id IS NULL
                THEN 'unresolved_analytic'
            WHEN s.pole_count_per_analytic > 1
                THEN 'ambiguous_multiple_fields'
            WHEN s.structure_deletion_mark
                THEN 'resolved_one_field_deleted'
            WHEN s.season_year_from_structure IS NULL
                THEN 'resolved_without_season_in_structure'
            WHEN h.pole_id IS NULL
                THEN 'resolved_without_field_history'
            ELSE 'resolved_one_field_active'
        END AS link_status
    FROM raw.r1c_putevoy_list_lines p
    LEFT JOIN structure_map s
      ON s.analitika_raskhodov_id = p.analitika_raskhodov_id::text
    LEFT JOIN field_history h
      ON h.pole_id = s.pole_id
     AND h.season_year = s.season_year_from_structure
)
SELECT
    base.*,
    'raw.r1c_putevoy_list_lines'
        || ' -> raw.r1c_struktura_predpriyatiya'
        || ' -> raw.r1c_polya_istoriya' AS link_source,
    now() AS _transformed_at
FROM base;

COMMENT ON VIEW staging.stg_waybill_field_link IS
    'Мост ПУЛ тракториста к полю/сезону/культуре через analitika_raskhodov_id и структуру предприятия. ГСМ в источнике нормативное, не фактическое.';

GRANT SELECT ON staging.stg_waybill_field_link TO datalens_ro;
