-- Чистая нормализованная витрина полного справочника номенклатуры 1С.
-- Legacy raw.r1c_nomenclature намеренно не изменяется:
-- в ней 133 исходные text-колонки ранее загруженной широкой выгрузки.

CREATE TABLE IF NOT EXISTS raw.r1c_nomenclature_current (
    _id                  text PRIMARY KEY,
    _deletionmark        boolean,
    parent_id            text,
    is_folder            boolean,
    code                 text,
    description          text,
    unit_id              text,
    nomenclature_type_id text,
    _loaded_at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_r1c_nomenclature_current_parent_id
    ON raw.r1c_nomenclature_current (parent_id);

CREATE INDEX IF NOT EXISTS ix_r1c_nomenclature_current_type_id
    ON raw.r1c_nomenclature_current (nomenclature_type_id);

CREATE INDEX IF NOT EXISTS ix_r1c_nomenclature_current_description
    ON raw.r1c_nomenclature_current (description);

COMMENT ON TABLE raw.r1c_nomenclature_current IS
    'Нормализованный инкрементально обновляемый справочник 1С Catalog_Номенклатура. Источник для закупок, СЗР, удобрений, семян и ГСМ.';
