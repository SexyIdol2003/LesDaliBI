-- 44_mart_fuel_purchase_by_month.sql
-- Закупки товарных ГСМ по месяцу, поставщику и виду топлива.
--
-- Включает:
--   diesel, gasoline, engine_oil.
--
-- Не включает:
--   топливные фильтры, насосы, датчики, генераторы, отопители,
--   запчасти и прочее оборудование.
--
-- ВАЖНО:
-- Витрина отражает закупки, а не фактическое списание в поле.
-- Не использовать для расчёта себестоимости конкретного поля,
-- пока не восстановлена связь путевой лист → поле.

CREATE OR REPLACE VIEW mart.v_fuel_purchase_by_month AS
SELECT
    date_trunc('month', h.doc_date)::date AS purchase_month,
    EXTRACT(YEAR FROM h.doc_date)::int AS purchase_year,
    EXTRACT(MONTH FROM h.doc_date)::int AS purchase_month_num,

    c.input_category,
    c.fuel_kind,

    COALESCE(
        contractor._id::text,
        '00000000-0000-0000-0000-000000000000'
    ) AS contractor_id,
    COALESCE(
        contractor.description,
        '(контрагент не найден)'
    ) AS contractor_name,

    c.nomenklatura_id,
    c.nomenclature_name,
    c.nomenclature_code,

    COUNT(*) AS purchase_lines,
    COUNT(DISTINCT h._id) AS purchase_documents,

    SUM(COALESCE(pl.kolichestvo, 0)) AS quantity,
    SUM(COALESCE(pl.summa, 0)) AS amount_without_vat_rub,
    SUM(COALESCE(pl.summa_nds, 0)) AS vat_rub,
    SUM(COALESCE(pl.summa_s_nds, 0)) AS amount_with_vat_rub,

    CASE
        WHEN SUM(COALESCE(pl.kolichestvo, 0)) <> 0
        THEN ROUND(
            SUM(COALESCE(pl.summa, 0))
            / SUM(COALESCE(pl.kolichestvo, 0)),
            4
        )
    END AS weighted_unit_price_without_vat,

    CASE
        WHEN SUM(COALESCE(pl.kolichestvo, 0)) <> 0
        THEN ROUND(
            SUM(COALESCE(pl.summa_s_nds, 0))
            / SUM(COALESCE(pl.kolichestvo, 0)),
            4
        )
    END AS weighted_unit_price_with_vat,

    MIN(h.doc_date)::date AS first_purchase_date,
    MAX(h.doc_date)::date AS last_purchase_date
FROM raw.r1c_purchase_lines pl
JOIN raw.r1c_purchase_headers h
  ON h._id = pl.doc_id
JOIN raw.v_purchase_nomenclature_classification c
  ON c.nomenklatura_id = pl.nomenklatura_id::text
LEFT JOIN raw.r1c_contractors contractor
  ON contractor._id = h.kontragent_id
WHERE COALESCE(h._deletionmark, false) = false
  AND COALESCE(h._posted, false) = true
  AND COALESCE(c.is_folder, false) = false
  AND COALESCE(c._deletionmark, false) = false
  AND c.input_category = 'ГСМ'
  AND c.fuel_kind IN ('diesel', 'gasoline', 'engine_oil')
GROUP BY
    date_trunc('month', h.doc_date)::date,
    EXTRACT(YEAR FROM h.doc_date)::int,
    EXTRACT(MONTH FROM h.doc_date)::int,
    c.input_category,
    c.fuel_kind,
    contractor._id,
    contractor.description,
    c.nomenklatura_id,
    c.nomenclature_name,
    c.nomenclature_code;

GRANT SELECT ON mart.v_fuel_purchase_by_month TO datalens_ro;

COMMENT ON VIEW mart.v_fuel_purchase_by_month IS
    'Закупки товарных ГСМ по месяцам/поставщикам/видам: diesel, gasoline, engine_oil. Не является фактическим списанием ГСМ по полям.';
