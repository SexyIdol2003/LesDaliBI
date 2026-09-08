-- ============================================================================
-- 20_raw_polya_istoriya.sql
-- Дополняем raw.r1c_polya реальными полями 1С и создаём raw-таблицу для
-- табличной части "ИсторияПоля" (площадь поля по годам урожая/культурам).
-- Подтверждено вручную 2026-09-08 живым запросом к Catalog_АпкПоля
-- (см. SESSION_2026-09-08_POLYA_AREA_DISCOVERY.md). Старое предположение
-- о плоском поле АпкПлощадьПоляГа не подтвердилось — такого поля нет.
-- ============================================================================

ALTER TABLE raw.r1c_polya ADD COLUMN IF NOT EXISTS organization_id text;
ALTER TABLE raw.r1c_polya ADD COLUMN IF NOT EXISTS efis_number text;
ALTER TABLE raw.r1c_polya ADD COLUMN IF NOT EXISTS sklad_semyan_id text;
ALTER TABLE raw.r1c_polya ADD COLUMN IF NOT EXISTS sklad_udobreniy_id text;
ALTER TABLE raw.r1c_polya ADD COLUMN IF NOT EXISTS sklad_szr_id text;
ALTER TABLE raw.r1c_polya ADD COLUMN IF NOT EXISTS sklad_produkcii_id text;
ALTER TABLE raw.r1c_polya ADD COLUMN IF NOT EXISTS sklad_prochih_id text;

COMMENT ON COLUMN raw.r1c_polya.area_ha IS
    'НЕ ЗАПОЛНЯЕТСЯ: такого плоского поля в 1С нет. Площадь — только в raw.r1c_polya_istoriya по годам.';

CREATE TABLE IF NOT EXISTS raw.r1c_polya_istoriya (
    _id                     text PRIMARY KEY,           -- {pole_id}_{LineNumber}
    pole_id                 text NOT NULL REFERENCES raw.r1c_polya(_id),
    line_number             int,
    god_urozhaya            int,                        -- ГодУрожая
    kultura_id              text,                        -- Культура (Ref_Key -> Catalog_АпкКультуры)
    ploshad_obshaya         numeric(12,4),               -- ПлощадьОбщая, га — ОСНОВНОЕ ПОЛЕ ДЛЯ РАСЧЁТОВ
    ploshad_seva            numeric(12,4),               -- ПлощадьСева, га
    ploshad_kontura         numeric(12,4),               -- ПлощадьКонтура, га (почти всегда 0)
    nachalo_perioda         timestamp,                   -- НачалоПериодаАктуальности
    konec_perioda           timestamp,                   -- КонецПериодаАктуальности
    predshestvennik_id      text,
    predshestvennik_text    text,                        -- часто "Пар"
    podrazdelenie_id        text,
    ne_ispolzuetsya         boolean,
    _loaded_at              timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_polya_istoriya_pole ON raw.r1c_polya_istoriya (pole_id);
CREATE INDEX IF NOT EXISTS ix_polya_istoriya_god  ON raw.r1c_polya_istoriya (god_urozhaya);

-- ---- Витрина: площадь поля по сезону/культуре (для yield_t_ha и себестоимости на га) ----
CREATE OR REPLACE VIEW mart.v_field_area_by_season AS
SELECT
    h.pole_id,
    p.description AS field_name,
    h.god_urozhaya,
    h.kultura_id,
    h.ploshad_obshaya AS area_ha,
    h.nachalo_perioda,
    h.konec_perioda
FROM raw.r1c_polya_istoriya h
JOIN raw.r1c_polya p ON p._id = h.pole_id
WHERE COALESCE(h.ne_ispolzuetsya, false) = false;

GRANT SELECT ON mart.v_field_area_by_season TO datalens_ro;
