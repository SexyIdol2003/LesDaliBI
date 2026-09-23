-- 39_raw_contractors_and_purchase_contract.sql
-- Справочник контрагентов (для классификации закупок по поставщику:
-- ГСМ/СЗР/удобрения по имени поставщика, а не только по номенклатуре/складу)
-- + добавляем ссылку на договор в шапку закупок (видна в форме документа:
-- "Договор № ФТГ-34/08/2021 от 30.08.2021" -- полезный вторичный признак).

CREATE TABLE IF NOT EXISTS raw.r1c_contractors (
    _id                    uuid PRIMARY KEY,
    _deletionmark          boolean,
    description            text,
    inn                    text,
    kpp                    text,
    legal_or_individual    text,
    _loaded_at             timestamp NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_r1c_contractors_description
    ON raw.r1c_contractors (description);

GRANT SELECT, INSERT, UPDATE ON raw.r1c_contractors TO dbt_runner;

-- Договор в шапке закупки (для будущей выгрузки; сам DAG dag_extract_purchases.py
-- нужно будет дополнить полем "Договор_Key" в $select, тогда колонка начнёт
-- заполняться при следующем запуске).
ALTER TABLE raw.r1c_purchase_headers
    ADD COLUMN IF NOT EXISTS dogovor_id uuid;

-- Быстрый обзор: сумма закупок по поставщику за всю историю — сразу видно,
-- где топливные/агрохимические контрагенты и сколько денег через них прошло.
CREATE OR REPLACE VIEW mart.v_purchase_summary_by_contractor AS
SELECT
    c.description                       AS contractor_name,
    EXTRACT(YEAR FROM h.doc_date)::int  AS year,
    COUNT(DISTINCT h._id)               AS documents,
    COUNT(*)                            AS lines,
    SUM(l.summa)                        AS total_summa
FROM raw.r1c_purchase_lines l
JOIN raw.r1c_purchase_headers h ON h._id = l.doc_id
LEFT JOIN raw.r1c_contractors c ON c._id = h.kontragent_id
GROUP BY c.description, EXTRACT(YEAR FROM h.doc_date)::int;

GRANT SELECT ON mart.v_purchase_summary_by_contractor TO datalens_ro;
