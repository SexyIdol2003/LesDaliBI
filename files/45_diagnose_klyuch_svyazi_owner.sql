-- 45_diagnose_klyuch_svyazi_owner.sql
-- Диагностика связи путевых листов с производственной аналитикой.
--
-- Результат проверки от 2026-09-23:
-- raw.r1c_putevoy_list_lines.pole_id существует в структуре,
-- но не заполнен ни в одной из строк.
-- Восстановление поля возможно только через владельца klyuch_svyazi
-- либо через другую опубликованную сущность 1С.

-- 1. Покрытие путевых листов полем.
SELECT
    COUNT(*) AS total_lines,
    COUNT(*) FILTER (
        WHERE p.pole_id IS NOT NULL
          AND btrim(p.pole_id) <> ''
          AND p.pole_id <> '00000000-0000-0000-0000-000000000000'
    ) AS lines_with_pole_id,
    COUNT(*) FILTER (
        WHERE p.pole_id IS NULL
           OR btrim(p.pole_id) = ''
           OR p.pole_id = '00000000-0000-0000-0000-000000000000'
    ) AS lines_without_pole_id,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE p.pole_id IS NOT NULL
              AND btrim(p.pole_id) <> ''
              AND p.pole_id <> '00000000-0000-0000-0000-000000000000'
        ) / NULLIF(COUNT(*), 0),
        2
    ) AS pole_id_coverage_pct
FROM raw.r1c_putevoy_list_lines p;

-- 2. Кардинальность ключа связи.
-- Один klyuch_svyazi может включать много документов и операций,
-- поэтому мост klyuch_svyazi -> pole_id нельзя строить без проверки
-- однозначности связи в сущности-владельце ключа.
SELECT
    p.klyuch_svyazi::text AS klyuch_svyazi,
    COUNT(*) AS line_count,
    COUNT(DISTINCT p.doc_id) AS put_list_documents,
    COUNT(DISTINCT NULLIF(btrim(p.agr_operaciya_id), '')) AS distinct_operations,
    MIN(p.den_raboty) AS first_work_date,
    MAX(p.den_raboty) AS last_work_date,
    SUM(COALESCE(p.gektarov, 0)) AS hectares,
    SUM(COALESCE(p.normativny_raskhod_gsm, 0)) AS normative_fuel
FROM raw.r1c_putevoy_list_lines p
WHERE p.klyuch_svyazi IS NOT NULL
GROUP BY p.klyuch_svyazi
ORDER BY line_count DESC, last_work_date DESC NULLS LAST;
