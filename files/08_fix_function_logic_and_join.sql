-- 08_fix_function_logic_and_join.sql
-- Исправление 2 итоговых багов:
-- 1) extract_field_number_prefixed возвращал исходный текст целиком, если префикса "Поле "
--    не было (REGEXP_REPLACE без совпадения просто отдаёт вход как есть, а не NULL).
--    Из-за этого COALESCE в extract_field_number_any всегда брал этот "ложный" результат
--    и не доходил до варианта с SPLIT_PART по запятой.
-- 2) raw.r1c_field_writeoff_lines связана с doc через doc_id (не doc_ref),
--    и первичный ключ raw.r1c_field_writeoff_doc — _id (не doc_ref).
--    Подтверждено \d-выводом от 2026-08-17.

CREATE OR REPLACE FUNCTION staging.extract_field_number_prefixed(input_text text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN input_text ~* '^Поле\s+'
        THEN NULLIF(TRIM(REGEXP_REPLACE(input_text, '^Поле\s+', '', 'i')), '')
        ELSE NULL
    END
$$;

-- Пересоздаём view с правильным JOIN (doc_id -> _id, а не doc_ref)
CREATE OR REPLACE VIEW staging.v_field_writeoff_clean AS
SELECT
    d._id,
    d.doc_number,
    d.doc_date,
    d.otpravitel_text,
    d.poluchatel_text,
    d.operaciya,
    staging.extract_field_number_any(d.otpravitel_text) AS pole_nomer,
    p._id AS pole_id,
    l.nomenklatura_id,
    l.kolichestvo,
    l.edinica_id
FROM raw.r1c_field_writeoff_doc d
JOIN raw.r1c_field_writeoff_lines l ON l.doc_id = d._id
LEFT JOIN raw.r1c_polya p
    ON p.description = staging.extract_field_number_any(d.otpravitel_text)
WHERE COALESCE(d._deletionmark, false) = false;
