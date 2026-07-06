-- =============================================================
-- 05_mart_facts.sql  —  MART-слой: факты по 5 объектам OData
-- Обновлено: 2026-07-06
-- =============================================================

CREATE SCHEMA IF NOT EXISTS mart;

-- 1. mart.fact_spisanie_materialov
CREATE TABLE IF NOT EXISTS mart.fact_spisanie_materialov (
    id                  BIGSERIAL PRIMARY KEY,
    doc_id              TEXT NOT NULL,
    doc_number          TEXT,
    doc_date            TIMESTAMPTZ,
    pole_id             TEXT REFERENCES raw.r1c_polya(_id),
    nomenklatura_id     TEXT,
    edinica_id          TEXT REFERENCES raw.r1c_upakovki(_id),
    kolichestvo         NUMERIC(18,3),
    summa               NUMERIC(18,2),
    seriya              TEXT,
    _updated_at         TIMESTAMPTZ DEFAULT now(),
    UNIQUE (doc_id, pole_id, nomenklatura_id)
);
CREATE INDEX IF NOT EXISTS idx_fact_spisanie_pole ON mart.fact_spisanie_materialov (pole_id);
CREATE INDEX IF NOT EXISTS idx_fact_spisanie_date ON mart.fact_spisanie_materialov (doc_date);

-- 2. mart.fact_vypusk_urozhaya
CREATE TABLE IF NOT EXISTS mart.fact_vypusk_urozhaya (
    id                  BIGSERIAL PRIMARY KEY,
    doc_id              TEXT NOT NULL,
    doc_number          TEXT,
    doc_date            TIMESTAMPTZ,
    pole_id             TEXT REFERENCES raw.r1c_polya(_id),
    nomenklatura_id     TEXT,
    edinica_id          TEXT REFERENCES raw.r1c_upakovki(_id),
    kolichestvo         NUMERIC(18,3),
    summa               NUMERIC(18,2),
    seriya              TEXT,
    _updated_at         TIMESTAMPTZ DEFAULT now(),
    UNIQUE (doc_id, pole_id, nomenklatura_id)
);
CREATE INDEX IF NOT EXISTS idx_fact_vypusk_pole ON mart.fact_vypusk_urozhaya (pole_id);
CREATE INDEX IF NOT EXISTS idx_fact_vypusk_date ON mart.fact_vypusk_urozhaya (doc_date);

-- 3. mart.fact_putevoy_rabota
CREATE TABLE IF NOT EXISTS mart.fact_putevoy_rabota (
    id                      BIGSERIAL PRIMARY KEY,
    doc_id                  TEXT NOT NULL,
    doc_number              TEXT,
    doc_date                TIMESTAMPTZ,
    tehnika_id              TEXT,
    model_tehniki_id        TEXT REFERENCES raw.r1c_sh_tehnika(_id),
    voditel_id              TEXT,
    pole_id                 TEXT REFERENCES raw.r1c_polya(_id),
    agr_operaciya_id        TEXT,
    edinica_id              TEXT REFERENCES raw.r1c_upakovki(_id),
    obem_rabot_ga           NUMERIC(12,4),
    norma_vyrabotki         NUMERIC(10,3),
    narabotka_moto_chas     NUMERIC(10,2),
    probeg_km               NUMERIC(10,2),
    toplivo_vydano          NUMERIC(12,3),
    toplivo_vozvrat         NUMERIC(12,3),
    _updated_at             TIMESTAMPTZ DEFAULT now(),
    UNIQUE (doc_id, pole_id, agr_operaciya_id)
);
CREATE INDEX IF NOT EXISTS idx_fact_putevoy_pole ON mart.fact_putevoy_rabota (pole_id);
CREATE INDEX IF NOT EXISTS idx_fact_putevoy_tehnika ON mart.fact_putevoy_rabota (model_tehniki_id);
CREATE INDEX IF NOT EXISTS idx_fact_putevoy_date ON mart.fact_putevoy_rabota (doc_date);

-- 4. Dim-витрины для BI
CREATE OR REPLACE VIEW mart.dim_polya AS
SELECT _id AS pole_id, description AS pole_name, parent_id, area_ha, cadastr_num, farm_id
FROM raw.r1c_polya WHERE _deletionmark = FALSE;

CREATE OR REPLACE VIEW mart.dim_sh_tehnika AS
SELECT _id AS tehnika_model_id, description AS model_name, manufacturer, equipment_class, power_hp
FROM raw.r1c_sh_tehnika WHERE _deletionmark = FALSE;

CREATE OR REPLACE VIEW mart.dim_upakovki AS
SELECT _id AS edinica_id, description AS edinica_name, code, weight_kg, volume_l
FROM raw.r1c_upakovki WHERE _deletionmark = FALSE;
