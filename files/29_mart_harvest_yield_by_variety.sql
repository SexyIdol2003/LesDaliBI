-- 29_mart_harvest_yield_by_variety.sql (fix 2: DROP VIEW CASCADE перед пересозданием)
-- Т/га по полю + сорту.
--
-- ИСПРАВЛЕНО (2026-09-15, третий раунд): CREATE OR REPLACE VIEW не может менять
-- состав/порядок колонок существующей view -- нужен явный DROP ... CASCADE
-- перед пересозданием (этот же урок уже был зафиксирован в 08_fix_function_logic_and_join.sql
-- для mart.v_fact_harvest_yield, повторно наступили на те же грабли).
--
-- Сезон определяется через диапазон дат [nachalo_perioda, konec_perioda], а не
-- через raw.r1c_polya_istoriya.god_urozhaya (почти всегда 0, невалидное поле).
--
-- НАХОДКА: raw.r1c_polya_istoriya.kultura_id -- GUID на raw.r1c_crops, чьё поле
-- description уже содержит культуру+сорт вместе ("Картофель Гала РС1 (свои)").
--
-- ОГРАНИЧЕНИЕ ПОКРЫТИЯ: из 747 строк raw.r1c_polya_istoriya активными
-- (eto_roditel=false, area>0) оказались только 85, из них лишь ~11 -- реальные
-- привязки к конкретному сорту (остальное -- "Заросли"/"Пар"/"Сидераты"/"ВЗО"). Фактически получено 38 из 74 строк с площадью (~51% покрытие), значения
-- yield_t_ha в диапазоне 20-60 т/га — реалистично для картофеля.
-- Используйте mart.v_fact_harvest_yield (поле+год) как основную витрину,
-- эту -- как дополнение там, где сорт реально зафиксирован в истории поля.

DROP VIEW IF EXISTS mart.v_fact_harvest_yield_by_variety CASCADE;
DROP VIEW IF EXISTS mart.v_field_area_by_variety CASCADE;

CREATE VIEW mart.v_field_area_by_variety AS
WITH crop_parsed AS (
    SELECT
        c._id AS kultura_id,
        c.description AS crop_description,
        CASE
            WHEN c.description ILIKE '%балтик роуз%' OR c.description ILIKE '%северное сияние%'
              OR c.description ILIKE '%беллароза%' OR c.description ILIKE '%белароза%'
              OR c.description ILIKE '%рэд скарлет%' OR c.description ILIKE '%ред соня%'
              OR c.description ILIKE '%рэд соня%' OR c.description ILIKE '%коломба%'
              OR c.description ILIKE '%фламинго%' OR c.description ILIKE '%мадейра%'
              OR c.description ILIKE '%кармен%' OR c.description ILIKE '%индиго%'
              OR c.description ILIKE '%аметист%' OR c.description ILIKE '%вэнди%'
              OR c.description ILIKE '%венди%' OR c.description ILIKE '%вега%'
              OR c.description ILIKE '%гала%' OR c.description ILIKE '%картофел%'
                THEN 'Картофель'
            WHEN c.description ILIKE '%чеснок%' THEN 'Чеснок'
            WHEN c.description ILIKE '%свекл%' THEN 'Свекла'
            WHEN c.description ILIKE '%рожь%' THEN 'Рожь'
            WHEN c.description ILIKE '%морков%' THEN 'Морковь'
            WHEN c.description ILIKE '%топинамбур%' THEN 'Топинамбур'
            WHEN c.description ILIKE '%многолетние травы%' THEN 'Многолетние травы'
            ELSE 'Не определено'
        END AS kultura,
        CASE
            WHEN c.description ILIKE '%балтик роуз%' THEN 'Балтик Роуз'
            WHEN c.description ILIKE '%северное сияние%' THEN 'Северное Сияние'
            WHEN c.description ILIKE '%беллароза%' OR c.description ILIKE '%белароза%' THEN 'Беллароза'
            WHEN c.description ILIKE '%рэд скарлет%' THEN 'Рэд Скарлет'
            WHEN c.description ILIKE '%ред соня%' OR c.description ILIKE '%рэд соня%' THEN 'Ред Соня'
            WHEN c.description ILIKE '%коломба%' THEN 'Коломба'
            WHEN c.description ILIKE '%фламинго%' THEN 'Фламинго'
            WHEN c.description ILIKE '%мадейра%' THEN 'Мадейра'
            WHEN c.description ILIKE '%кармен%' THEN 'Кармен'
            WHEN c.description ILIKE '%индиго%' THEN 'Индиго'
            WHEN c.description ILIKE '%аметист%' THEN 'Аметист'
            WHEN c.description ILIKE '%вэнди%' OR c.description ILIKE '%венди%' THEN 'Вэнди'
            WHEN c.description ILIKE '%вега%' THEN 'Вега'
            WHEN c.description ILIKE '%гала%' THEN 'Гала'
            ELSE NULL
        END AS sort
    FROM raw.r1c_crops c
)
SELECT
    h.pole_id,
    p.description AS field_name,
    cp.kultura,
    cp.sort,
    cp.crop_description,
    h.ploshad_obshaya AS area_ha,
    LEAST(h.nachalo_perioda, h.konec_perioda) AS period_start,
    GREATEST(h.nachalo_perioda, h.konec_perioda) AS period_end
FROM raw.r1c_polya_istoriya h
JOIN raw.r1c_polya p ON p._id = h.pole_id
JOIN crop_parsed cp ON cp.kultura_id = h.kultura_id
WHERE COALESCE(h.eto_roditel, false) = false
  AND h.ploshad_obshaya > 0
  AND h.nachalo_perioda > '0002-01-01'::timestamp
  AND h.konec_perioda > '0002-01-01'::timestamp
  AND cp.kultura <> 'Не определено';

COMMENT ON VIEW mart.v_field_area_by_variety IS
    'Площадь поля в разрезе культуры/сорта с диапазоном действия периода, через '
    'GUID-джойн raw.r1c_polya_istoriya.kultura_id -> raw.r1c_crops. '
    'Покрытие ограничено: из 747 строк истории поля только 85 активны и лишь '
    '~11 из них -- конкретные сорта. Создано 2026-09-15.';

CREATE VIEW mart.v_fact_harvest_yield_by_variety AS
WITH harvest_by_field_variety AS (
    SELECT
        pole_id,
        god_urozhaya,
        kultura,
        sort,
        MIN(doc_date) AS first_doc_date,
        SUM(kolichestvo_kg) AS total_kg,
        COUNT(*) AS weighings_cnt
    FROM mart.v_fact_harvest_enriched
    WHERE is_pole_resolved AND kultura <> 'Не определено'
    GROUP BY pole_id, god_urozhaya, kultura, sort
),
area_matched AS (
    SELECT
        hv.pole_id,
        hv.god_urozhaya,
        hv.kultura,
        hv.sort,
        MAX(a.field_name) AS field_name,
        SUM(a.area_ha) AS area_ha
    FROM harvest_by_field_variety hv
    JOIN mart.v_field_area_by_variety a
        ON a.pole_id = hv.pole_id
       AND a.kultura = hv.kultura
       AND a.sort IS NOT DISTINCT FROM hv.sort
       AND hv.first_doc_date::date BETWEEN a.period_start::date AND a.period_end::date
    GROUP BY hv.pole_id, hv.god_urozhaya, hv.kultura, hv.sort
)
SELECT
    hv.pole_id,
    am.field_name,
    hv.god_urozhaya,
    hv.kultura,
    hv.sort,
    hv.total_kg,
    ROUND(hv.total_kg / 1000.0, 3) AS total_t,
    hv.weighings_cnt,
    am.area_ha,
    CASE WHEN am.area_ha IS NULL OR am.area_ha = 0 THEN NULL
         ELSE ROUND((hv.total_kg / 1000.0) / am.area_ha, 3)
    END AS yield_t_ha
FROM harvest_by_field_variety hv
LEFT JOIN area_matched am
    ON am.pole_id = hv.pole_id
   AND am.god_urozhaya = hv.god_urozhaya
   AND am.kultura = hv.kultura
   AND am.sort IS NOT DISTINCT FROM hv.sort;

COMMENT ON VIEW mart.v_fact_harvest_yield_by_variety IS
    'Факт сбора урожая: поле - сорт - т/га. Площадь через диапазон дат '
    '(как в v_fact_harvest_yield). area_ha IS NULL -- нет записи в истории '
    'поля для этого сорта/сезона, используйте total_t. Создано 2026-09-15.';

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'datalens_ro') THEN
        EXECUTE 'GRANT SELECT ON mart.v_field_area_by_variety TO datalens_ro';
        EXECUTE 'GRANT SELECT ON mart.v_fact_harvest_yield_by_variety TO datalens_ro';
    END IF;
END $$;
