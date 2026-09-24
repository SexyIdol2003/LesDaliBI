-- ============================================================================
-- 54_mart_fuel_writeoff_purchase_price_coverage.sql
--
-- Расчётная оценка стоимости списанного дизельного топлива по закупочной
-- цене той же номенклатуры в том же месяце.
--
-- Гранулярность:
--   месяц × марка топлива (fuel_brand_id).
--
-- Метод:
--   1. Берутся фактические литры mart.fact_fuel_writeoff.
--   2. Берутся закупки mart.v_fuel_purchase_by_month.
--   3. Соответствие марки списания и закупочной номенклатуры — точное:
--      fuel_brand_id = nomenklatura_id.
--   4. Цена — средневзвешенная закупочная цена без НДС за тот же месяц
--      и для той же номенклатуры.
--   5. Если закупки той же номенклатуры в месяце нет, цена и стоимость
--      не заполняются. Не используются прошлые месяцы, другие марки,
--      годовые средние или неявная дооценка.
--
-- Ограничения:
--   * закупочная цена — расчётная оценка, не первичная стоимость списания;
--   * в текущем факте списаний присутствуют только дизельные марки;
--   * нельзя показывать сумму как полную стоимость ГСМ без price_coverage_pct;
--   * не является витриной полной себестоимости поля или техники.
-- ============================================================================

DROP VIEW IF EXISTS mart.v_fuel_writeoff_purchase_price_coverage_quality;
DROP VIEW IF EXISTS mart.v_fuel_writeoff_purchase_price_coverage;

CREATE VIEW mart.v_fuel_writeoff_purchase_price_coverage AS
WITH writeoff_by_month AS (
    SELECT
        f.period_month,
        f.fuel_brand_id::text AS fuel_brand_id,
        SUM(COALESCE(f.liters_consumed, 0)) AS writeoff_liters,
        COUNT(*) AS writeoff_rows,
        COUNT(DISTINCT f.equipment_sk) AS equipment_count
    FROM mart.fact_fuel_writeoff f
    WHERE f.period_month IS NOT NULL
      AND f.fuel_brand_id IS NOT NULL
    GROUP BY
        f.period_month,
        f.fuel_brand_id
),
purchase_by_month AS (
    SELECT
        p.purchase_month,
        p.nomenklatura_id::text AS fuel_brand_id,

        MAX(p.fuel_kind) AS fuel_kind,
        MAX(p.nomenclature_name) AS fuel_name,
        MAX(p.nomenclature_code) AS fuel_code,

        SUM(COALESCE(p.purchase_lines, 0)) AS purchase_lines,
        SUM(COALESCE(p.purchase_documents, 0)) AS purchase_documents,
        SUM(COALESCE(p.quantity, 0)) AS purchase_quantity,
        SUM(COALESCE(p.amount_without_vat_rub, 0)) AS purchase_amount_without_vat_rub,
        SUM(COALESCE(p.vat_rub, 0)) AS purchase_vat_rub,
        SUM(COALESCE(p.amount_with_vat_rub, 0)) AS purchase_amount_with_vat_rub,

        ROUND(
            SUM(COALESCE(p.amount_without_vat_rub, 0))
            / NULLIF(SUM(COALESCE(p.quantity, 0)), 0),
            4
        ) AS purchase_price_without_vat_rub_per_liter,

        ROUND(
            SUM(COALESCE(p.amount_with_vat_rub, 0))
            / NULLIF(SUM(COALESCE(p.quantity, 0)), 0),
            4
        ) AS purchase_price_with_vat_rub_per_liter,

        MIN(p.first_purchase_date) AS first_purchase_date,
        MAX(p.last_purchase_date) AS last_purchase_date
    FROM mart.v_fuel_purchase_by_month p
    WHERE p.fuel_kind = 'diesel'
    GROUP BY
        p.purchase_month,
        p.nomenklatura_id
),
joined AS (
    SELECT
        w.period_month,
        EXTRACT(YEAR FROM w.period_month)::integer AS season_year,

        w.fuel_brand_id,
        COALESCE(
            p.fuel_name,
            'Марка дизеля без закупки в соответствующем месяце'
        ) AS fuel_name,
        COALESCE(p.fuel_code, '') AS fuel_code,
        COALESCE(p.fuel_kind, 'diesel') AS fuel_kind,

        w.writeoff_rows,
        w.equipment_count,
        w.writeoff_liters,

        p.purchase_lines,
        p.purchase_documents,
        p.purchase_quantity,
        p.purchase_amount_without_vat_rub,
        p.purchase_vat_rub,
        p.purchase_amount_with_vat_rub,
        p.purchase_price_without_vat_rub_per_liter,
        p.purchase_price_with_vat_rub_per_liter,
        p.first_purchase_date,
        p.last_purchase_date,

        CASE
            WHEN p.purchase_quantity > 0
             AND p.purchase_amount_without_vat_rub > 0
                THEN 'OK_EXACT_MONTH_BRAND_PURCHASE_PRICE'
            ELSE 'NO_EXACT_MONTH_BRAND_PURCHASE_PRICE'
        END AS purchase_price_status
    FROM writeoff_by_month w
    LEFT JOIN purchase_by_month p
      ON p.purchase_month = w.period_month
     AND p.fuel_brand_id = w.fuel_brand_id
)
SELECT
    period_month,
    season_year,

    fuel_brand_id,
    fuel_name,
    fuel_code,
    fuel_kind,

    writeoff_rows,
    equipment_count,
    ROUND(writeoff_liters, 4) AS writeoff_liters,

    purchase_lines,
    purchase_documents,
    ROUND(purchase_quantity, 4) AS purchase_quantity,
    ROUND(purchase_amount_without_vat_rub, 2) AS purchase_amount_without_vat_rub,
    ROUND(purchase_vat_rub, 2) AS purchase_vat_rub,
    ROUND(purchase_amount_with_vat_rub, 2) AS purchase_amount_with_vat_rub,

    purchase_price_without_vat_rub_per_liter,
    purchase_price_with_vat_rub_per_liter,

    first_purchase_date,
    last_purchase_date,

    CASE
        WHEN purchase_price_status =
             'OK_EXACT_MONTH_BRAND_PURCHASE_PRICE'
        THEN ROUND(
            writeoff_liters
            * purchase_price_without_vat_rub_per_liter,
            2
        )
    END AS estimated_covered_fuel_cost_without_vat_rub,

    CASE
        WHEN purchase_price_status =
             'OK_EXACT_MONTH_BRAND_PURCHASE_PRICE'
        THEN ROUND(
            writeoff_liters
            * purchase_price_with_vat_rub_per_liter,
            2
        )
    END AS estimated_covered_fuel_cost_with_vat_rub,

    purchase_price_status,

    'PURCHASE_PRICE_EXACT_MONTH_BRAND'::text AS price_method,

    'mart.v_fuel_purchase_by_month: same month + exact nomenklatura UUID'
        AS price_source,

    CASE
        WHEN purchase_price_status =
             'OK_EXACT_MONTH_BRAND_PURCHASE_PRICE'
        THEN true
        ELSE false
    END AS has_exact_purchase_price,

    now() AS calculated_at
FROM joined;

COMMENT ON VIEW mart.v_fuel_writeoff_purchase_price_coverage IS
    'Расчётная оценка стоимости списанного дизеля по точной закупочной цене той же номенклатуры в том же месяце. Не является первичной стоимостью списания; при отсутствии закупки цена не дооценивается.';

CREATE VIEW mart.v_fuel_writeoff_purchase_price_coverage_quality AS
SELECT
    season_year,

    COUNT(*) AS month_brand_rows,
    COUNT(DISTINCT period_month) AS months_with_writeoff,
    COUNT(DISTINCT fuel_brand_id) AS fuel_brands,

    ROUND(SUM(writeoff_liters), 4) AS total_writeoff_liters,

    ROUND(
        SUM(writeoff_liters) FILTER (
            WHERE has_exact_purchase_price
        ),
        4
    ) AS writeoff_liters_with_exact_purchase_price,

    ROUND(
        SUM(writeoff_liters) FILTER (
            WHERE NOT has_exact_purchase_price
        ),
        4
    ) AS writeoff_liters_without_exact_purchase_price,

    ROUND(
        100.0
        * SUM(writeoff_liters) FILTER (
            WHERE has_exact_purchase_price
        )
        / NULLIF(SUM(writeoff_liters), 0),
        2
    ) AS price_coverage_pct,

    COUNT(*) FILTER (
        WHERE has_exact_purchase_price
    ) AS month_brand_rows_with_price,

    COUNT(*) FILTER (
        WHERE NOT has_exact_purchase_price
    ) AS month_brand_rows_without_price,

    ROUND(
        SUM(estimated_covered_fuel_cost_without_vat_rub),
        2
    ) AS estimated_covered_fuel_cost_without_vat_rub,

    ROUND(
        SUM(estimated_covered_fuel_cost_with_vat_rub),
        2
    ) AS estimated_covered_fuel_cost_with_vat_rub,

    CASE
        WHEN SUM(writeoff_liters) = 0
            THEN 'NO_WRITEOFF_LITERS'
        WHEN SUM(writeoff_liters) FILTER (
            WHERE has_exact_purchase_price
        ) = SUM(writeoff_liters)
            THEN 'FULL_EXACT_PURCHASE_PRICE_COVERAGE'
        WHEN SUM(writeoff_liters) FILTER (
            WHERE has_exact_purchase_price
        ) > 0
            THEN 'PARTIAL_EXACT_PURCHASE_PRICE_COVERAGE'
        ELSE 'NO_EXACT_PURCHASE_PRICE_COVERAGE'
    END AS price_coverage_status,

    'PURCHASE_PRICE_EXACT_MONTH_BRAND'::text AS price_method,

    now() AS calculated_at
FROM mart.v_fuel_writeoff_purchase_price_coverage
GROUP BY season_year;

COMMENT ON VIEW mart.v_fuel_writeoff_purchase_price_coverage_quality IS
    'QA покрытия списанного дизеля точной закупочной ценой той же номенклатуры и месяца. Показывает только покрытую расчётную стоимость; отсутствующие цены не дооцениваются.';

GRANT SELECT ON mart.v_fuel_writeoff_purchase_price_coverage TO datalens_ro;
GRANT SELECT ON mart.v_fuel_writeoff_purchase_price_coverage_quality TO datalens_ro;
