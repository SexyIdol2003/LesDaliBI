-- 43_v_purchase_nomenclature_classification.sql
-- Единая классификация номенклатуры закупок для BI.
--
-- Источник:
--   raw.r1c_nomenclature_current — полный актуальный справочник
--   Catalog_Номенклатура из 1С OData.
--
-- Вариант A для ГСМ:
--   diesel / gasoline / engine_oil входят в input_category = 'ГСМ',
--   но разделяются полем fuel_kind.
--
-- Не включаются в ГСМ:
--   фильтры, насосы, датчики, топливопроводы, форсунки, генераторы,
--   дизельные пушки/отопители, бензиновые триммеры и прочее оборудование.

CREATE OR REPLACE VIEW raw.v_purchase_nomenclature_classification AS
WITH source AS (
    SELECT
        n._id AS nomenklatura_id,
        n.description AS nomenclature_name,
        n.code AS nomenclature_code,
        n.parent_id,
        n.unit_id,
        n.nomenclature_type_id,
        n.is_folder,
        n._deletionmark,
        lower(btrim(COALESCE(n.description, ''))) AS name_lc
    FROM raw.r1c_nomenclature_current n
),
classified AS (
    SELECT
        s.*,

        CASE
            -- ГСМ: точные товарные группы.
            -- Проверяем раньше семян/прочих категорий.
            WHEN s.name_lc ~
                '^(дизельное[[:space:]]+топливо|топливо[[:space:]]+дизельное|газойл)'
                THEN 'diesel'

            WHEN s.name_lc ~
                '^бензин[[:space:]]+(аи|aи|экто|премиум)'
                THEN 'gasoline'

            WHEN s.name_lc ~
                '^(масло[[:space:]]+(моторное|дизель)|масло[[:space:]]+дизель)'
                THEN 'engine_oil'

            ELSE NULL
        END AS fuel_kind,

        CASE
            -- Семена: добавляем конкретные культуры, чтобы не потерять
            -- товарные позиции с неполным названием.
            WHEN s.name_lc ~
                '(семен|семенн|гибрид|картофель[[:space:]]+семенн|сем[[:space:]]+покупн)'
                THEN 'Семена'

            -- СЗР.
            WHEN s.name_lc ~
                '(гербицид|фунгицид|инсектицид|протравител|адьювант|акарицид|родентицид|десикант|регулятор[[:space:]]+роста|сзр)'
                THEN 'СЗР'

            -- Удобрения.
            WHEN s.name_lc ~
                '(удобрен|селитр|карбамид|аммофос|нитрат|азофоск|азофос|нитроаммофоск|диаммофоск|сульфат[[:space:]]+аммони|хлорист[[:space:]]+кал|монофосфат|суперфосфат|npk|тук)'
                THEN 'Удобрения'

            ELSE 'Прочее'
        END AS non_fuel_category
    FROM source s
)
SELECT
    nomenklatura_id,
    nomenclature_name,
    nomenclature_code,
    parent_id,
    unit_id,
    nomenclature_type_id,
    is_folder,
    _deletionmark,

    CASE
        WHEN fuel_kind IS NOT NULL THEN 'ГСМ'
        ELSE non_fuel_category
    END AS input_category,

    fuel_kind,

    CASE
        WHEN fuel_kind IN ('diesel', 'gasoline') THEN true
        ELSE false
    END AS is_liquid_fuel,

    CASE
        WHEN fuel_kind = 'engine_oil' THEN true
        ELSE false
    END AS is_engine_oil,

    CASE
        WHEN non_fuel_category = 'Семена'
             AND name_lc !~ '(корм[[:space:]]+пшениц|кормовое[[:space:]]+зерно)'
            THEN true
        ELSE false
    END AS is_seed_for_planting,

    CASE
        WHEN fuel_kind = 'diesel'
            THEN 'regex: diesel exact product prefix'
        WHEN fuel_kind = 'gasoline'
            THEN 'regex: gasoline exact product prefix'
        WHEN fuel_kind = 'engine_oil'
            THEN 'regex: engine oil exact product prefix'
        WHEN non_fuel_category = 'Семена'
            THEN 'regex: seed material'
        WHEN non_fuel_category = 'СЗР'
            THEN 'regex: crop protection'
        WHEN non_fuel_category = 'Удобрения'
            THEN 'regex: fertilizers'
        ELSE 'unclassified'
    END AS classification_rule,

    now() AS _classified_at
FROM classified;
