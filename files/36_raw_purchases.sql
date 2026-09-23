-- 36_raw_purchases.sql
-- Единые (year-agnostic) raw-таблицы закупок для дальнейшей денежной модели
-- агрохимии. Заполняются DAG-ом dag_extract_purchases (files/airflow/dags/).
-- Идемпотентная загрузка через ON CONFLICT ... DO UPDATE, без суффикса года.

CREATE TABLE IF NOT EXISTS raw.r1c_purchase_headers (
    _id                   uuid PRIMARY KEY,
    _deletionmark         boolean,
    _posted               boolean,
    doc_number            text,
    doc_date              timestamp,
    organizaciya_id       uuid,
    kontragent_id         uuid,
    cena_vklyuchaet_nds   boolean,
    _loaded_at            timestamp NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.r1c_purchase_lines (
    doc_id                  uuid NOT NULL,
    line_number             integer NOT NULL,
    nomenklatura_id         uuid,
    harakteristika_id       uuid,
    seriya_id               uuid,
    kolichestvo             numeric,
    cena                    numeric,
    summa                   numeric,
    stavka_nds_id           uuid,
    summa_nds               numeric,
    summa_s_nds             numeric,
    sklad_id                uuid,
    _loaded_at              timestamp NOT NULL DEFAULT now(),
    PRIMARY KEY (doc_id, line_number)
);

CREATE INDEX IF NOT EXISTS idx_r1c_purchase_headers_date
    ON raw.r1c_purchase_headers (doc_date);

CREATE INDEX IF NOT EXISTS idx_r1c_purchase_lines_nomenklatura
    ON raw.r1c_purchase_lines (nomenklatura_id);

CREATE INDEX IF NOT EXISTS idx_r1c_purchase_lines_doc_id
    ON raw.r1c_purchase_lines (doc_id);

-- Права по стандартной ролевой модели проекта (см. LesDaliBI_audit_summary.md)
GRANT SELECT, INSERT, UPDATE ON raw.r1c_purchase_headers TO dbt_runner;
GRANT SELECT, INSERT, UPDATE ON raw.r1c_purchase_lines TO dbt_runner;

-- Если исторические JSONL (purchase_headers_2021..2025.jsonl,
-- purchase_lines_2021..2025.jsonl) уже выгружены руками — их можно
-- одноразово догрузить в эти же таблицы (idempotent upsert), а дальше
-- DAG будет поддерживать данные свежими сам, без разбивки по годам.
