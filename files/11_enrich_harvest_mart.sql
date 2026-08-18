-- 11_enrich_harvest_mart.sql
-- Заполняет стандартный mart.fact_harvest из проверенной mart.v_fact_harvest_enriched.
-- Создаёт измерения культуры/сорта/полива и технические записи для GUID
-- номенклатуры, отсутствующих в текущем справочнике 1С.
-- Не изменяет raw. Площадь и yield_t_ha не загружаются: yield_t_ha generated column.

BEGIN;

WITH crop_source AS (
    SELECT DISTINCT
        concat_ws(' / ', kultura, COALESCE(sort, 'Сорт не указан')) AS variety,
        CASE
            WHEN nomenklatura_name ILIKE '%белый%'
              OR pole_party_name ILIKE '%белый%' THEN 'Белый'
            WHEN nomenklatura_name ILIKE '%красный%'
              OR pole_party_name ILIKE '%красный%' THEN 'Красный'
            ELSE NULL
        END AS color,
        NULLIF(reproduktsiya, '') AS reproduction,
        NULLIF(poliv, 'Не указан') AS irrigation
    FROM mart.v_fact_harvest_enriched
),
new_crop AS (
    SELECT s.*
    FROM crop_source s
    WHERE NOT EXISTS (
        SELECT 1
        FROM mart.dim_crop d
        WHERE d.variety = s.variety
          AND d.color IS NOT DISTINCT FROM s.color
          AND d.reproduction IS NOT DISTINCT FROM s.reproduction
          AND d.irrigation IS NOT DISTINCT FROM s.irrigation
    )
)
INSERT INTO mart.dim_crop (variety, color, reproduction, irrigation)
SELECT variety, color, reproduction, irrigation
FROM new_crop;

WITH missing_nom AS (
    SELECT DISTINCT ON (h.nomenklatura_id)
        h.nomenklatura_id::uuid AS ref_key_1c,
        COALESCE(
            NULLIF(h.nomenklatura_name, ''),
            NULLIF(h.pole_party_name, ''),
            'Номенклатура из взвешивания без наименования'
        ) AS name_1c
    FROM mart.v_fact_harvest_enriched h
    LEFT JOIN mart.dim_nomenclature n
        ON n.ref_key_1c::text = h.nomenklatura_id::text
    WHERE n.nom_sk IS NULL
      AND h.nomenklatura_id IS NOT NULL
    ORDER BY h.nomenklatura_id, h.doc_date DESC
)
INSERT INTO mart.dim_nomenclature (
    article_1c, name_1c, nom_kind, category, subcategory, crop_sk,
    caliber, pack_type, pack_size_kg, customer_label, is_active,
    ref_key_1c, code_1c
)
SELECT
    NULL, m.name_1c, 'Техническая запись',
    'Номенклатура не загружена из 1С', NULL, NULL,
    NULL, NULL, NULL, NULL, true, m.ref_key_1c, NULL
FROM missing_nom m
WHERE NOT EXISTS (
    SELECT 1
    FROM mart.dim_nomenclature n
    WHERE n.ref_key_1c = m.ref_key_1c
);

INSERT INTO mart.fact_harvest (
    date_day, field_sk, crop_sk, nom_sk, plot_no,
    area_ha, weight_kg, src_doc_ref
)
SELECT
    h.doc_date::date,
    f.field_sk,
    c.crop_sk,
    n.nom_sk,
    h.uchastok_nomer,
    NULL::numeric,
    h.kolichestvo_kg,
    h.src_doc_id::text
FROM mart.v_fact_harvest_enriched h
JOIN mart.dim_field f
    ON f.field_code_1c = h.pole_id::text
   AND f.is_current = true
JOIN mart.dim_nomenclature n
    ON n.ref_key_1c::text = h.nomenklatura_id::text
JOIN mart.dim_crop c
    ON c.variety = concat_ws(' / ', h.kultura, COALESCE(h.sort, 'Сорт не указан'))
   AND c.color IS NOT DISTINCT FROM CASE
        WHEN h.nomenklatura_name ILIKE '%белый%'
          OR h.pole_party_name ILIKE '%белый%' THEN 'Белый'
        WHEN h.nomenklatura_name ILIKE '%красный%'
          OR h.pole_party_name ILIKE '%красный%' THEN 'Красный'
        ELSE NULL
   END
   AND c.reproduction IS NOT DISTINCT FROM NULLIF(h.reproduktsiya, '')
   AND c.irrigation IS NOT DISTINCT FROM NULLIF(h.poliv, 'Не указан')
WHERE NOT EXISTS (
    SELECT 1
    FROM mart.fact_harvest fh
    WHERE fh.src_doc_ref = h.src_doc_id::text
);

COMMIT;

-- Контроль:
-- SELECT count(*) AS rows, count(DISTINCT src_doc_ref) AS docs,
--        round(sum(weight_kg) / 1000.0, 3) AS tons
-- FROM mart.fact_harvest;
