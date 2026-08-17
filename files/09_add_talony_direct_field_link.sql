-- 09_add_talony_direct_field_link.sql
-- Табличная часть "Талоны" документа Регистрации взвешиваний на току содержит
-- ПРЯМУЮ ссылку на поле (Поле_Key) — это надёжнее, чем текстовый парсинг "Откуда".
-- См. SESSION_2026-08-17: реальный JSON-дамп Document_АпкРегистрацияВзвешиванийНаТоку.

-- Исправляем структуру шапки под реальные имена полей 1С
ALTER TABLE raw.r1c_tok_vzveshivanie RENAME COLUMN otkuda_text TO otkuda_id;
ALTER TABLE raw.r1c_tok_vzveshivanie RENAME COLUMN kuda_text TO kuda_id;
ALTER TABLE raw.r1c_tok_vzveshivanie ALTER COLUMN otkuda_id TYPE uuid USING NULL;
ALTER TABLE raw.r1c_tok_vzveshivanie ALTER COLUMN kuda_id TYPE uuid USING NULL;
ALTER TABLE raw.r1c_tok_vzveshivanie RENAME COLUMN nomenklatura_text TO nomenklatura_id;
ALTER TABLE raw.r1c_tok_vzveshivanie ALTER COLUMN nomenklatura_id TYPE uuid USING NULL;
ALTER TABLE raw.r1c_tok_vzveshivanie RENAME COLUMN vesovshik_id TO avtor_id;

-- Новая табличная часть: Талоны — здесь лежит прямой Поле_Key
CREATE TABLE IF NOT EXISTS raw.r1c_tok_vzveshivanie_talony (
    _id                 text PRIMARY KEY,      -- doc_id || '-' || line_number
    doc_id              uuid NOT NULL REFERENCES raw.r1c_tok_vzveshivanie(_id),
    line_number         int,
    pole_id             uuid,                  -- Поле_Key — ПРЯМАЯ ссылка на raw.r1c_polya!
    nomer_talona        text,
    tip_talona          text,                  -- ТалонКомбайнера / ...
    massa_kg            numeric(18,3),
    kombayn_id          uuid,
    mehanizator_id      uuid,
    obem_bunkera        numeric(18,3),
    procent_zapolneniya numeric(6,2),
    _loaded_at          timestamp DEFAULT now()
);

-- staging: урожай по полю через ПРЯМУЮ связь (приоритетный источник)
CREATE OR REPLACE VIEW staging.v_tok_talony_clean AS
SELECT
    t.doc_id,
    d.doc_date,
    d.doc_number,
    t.pole_id,
    t.tip_talona,
    t.massa_kg,
    t.kombayn_id,
    t.mehanizator_id,
    EXTRACT(YEAR FROM d.doc_date)::int AS god_urozhaya
FROM raw.r1c_tok_vzveshivanie_talony t
JOIN raw.r1c_tok_vzveshivanie d ON d._id = t.doc_id
WHERE t.tip_talona = 'ТалонКомбайнера'
  AND t.pole_id IS NOT NULL
  AND t.pole_id <> '00000000-0000-0000-0000-000000000000'::uuid;
