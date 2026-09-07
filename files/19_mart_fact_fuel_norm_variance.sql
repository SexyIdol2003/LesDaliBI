-- ============================================================================
-- mart.fact_fuel_norm_variance
-- сравнение фактической заправки топлива (mart.fact_fuel_refuel, источник —
-- заправочные ведомости) с нормативным расходом из выполненных работ
-- (raw.r1c_putevoy_list_lines.normativny_raskhod_gsm, источник — путевые листы).
--
-- Почему гранулярность "месяц+техника", а не "день+техника":
-- заправка и расход топлива — разные по природе события
-- (залили ≠ сразу сожгли — в баке есть запас), сравнивать их имеет смысл
-- только на более длинном горизонте, где запас в баке сглаживается.
--
-- Источники и покрытие данных (проверено 2026-09-07 после full backfill
-- dag_extract_putevoy_list с --conf '{"date_from": "2021-01-01T00:00:00"}'):
--   факт: mart.fact_fuel_refuel, период 2021-07 .. 2026-07/08, 22 ед. техники
--   норма: raw.r1c_putevoy_list_lines, период 2021-07 .. 2026-08, 24 ед. техники,
--            normativny_raskhod_gsm заполнен в 17049 из 25696 строк (66%)
-- ============================================================================

DROP VIEW IF EXISTS mart.fact_fuel_norm_variance;

CREATE VIEW mart.fact_fuel_norm_variance AS
WITH norm_side AS (
    SELECT
        date_trunc('month', l.den_raboty)::date AS month_start,
        eq.eq_sk,
        SUM(l.normativny_raskhod_gsm)            AS norm_liters
    FROM raw.r1c_putevoy_list_lines l
    JOIN raw.r1c_putevoy_list doc
        ON doc._id = l.doc_id
    LEFT JOIN mart.dim_equipment eq
        ON eq.code_1c = doc.tehnika_id
    WHERE doc._posted = true
      AND (doc._deletionmark IS NULL OR doc._deletionmark = false)
      AND l.normativny_raskhod_gsm IS NOT NULL
      AND l.den_raboty IS NOT NULL
    GROUP BY 1, 2
),
fact_side AS (
    SELECT
        date_trunc('month', date_day)::date AS month_start,
        equipment_sk                        AS eq_sk,
        SUM(liters)                         AS fact_liters
    FROM mart.fact_fuel_refuel
    GROUP BY 1, 2
)
SELECT
    COALESCE(f.month_start, n.month_start)  AS month_start,
    COALESCE(f.eq_sk, n.eq_sk)              AS eq_sk,
    eq.name                                 AS tehnika_name,
    eq.eq_type                              AS tehnika_type,
    COALESCE(f.fact_liters, 0)              AS fact_liters,
    COALESCE(n.norm_liters, 0)              AS norm_liters,
    COALESCE(f.fact_liters, 0) - COALESCE(n.norm_liters, 0)               AS variance_liters,
    CASE WHEN COALESCE(n.norm_liters, 0) > 0
         THEN ROUND((COALESCE(f.fact_liters, 0) - n.norm_liters) / n.norm_liters * 100, 1)
         ELSE NULL
    END                                     AS variance_pct
FROM fact_side f
FULL OUTER JOIN norm_side n
    ON n.month_start = f.month_start AND n.eq_sk = f.eq_sk
LEFT JOIN mart.dim_equipment eq
    ON eq.eq_sk = COALESCE(f.eq_sk, n.eq_sk);

COMMENT ON VIEW mart.fact_fuel_norm_variance IS
    'Сравнение фактической заправки топлива (fact_fuel_refuel) с нормативным расходом '
    'из выполненных работ (putevoy_list_lines.normativny_raskhod_gsm), гранулярность месяц+техника. '
    'Позитивный variance_liters = перерасход относительно нормы. Создано 2026-09-07.';

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'datalens_ro') THEN
        EXECUTE 'GRANT SELECT ON mart.fact_fuel_norm_variance TO datalens_ro';
    END IF;
END $$;
