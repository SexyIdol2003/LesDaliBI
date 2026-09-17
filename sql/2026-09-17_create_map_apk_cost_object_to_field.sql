-- Создано в ходе сессии 2026-09-17.
-- Карта подтверждённых соответствий: объект затрат АПК 1С -> поле BI.
-- Скрипт идемпотентен. Он создаёт пустую таблицу и не вставляет
-- предположительные соответствия.

CREATE TABLE IF NOT EXISTS staging.map_apk_cost_object_to_field (
    apk_cost_object_key uuid PRIMARY KEY,
    field_sk bigint NOT NULL REFERENCES mart.dim_field(field_sk),
    mapping_source text NOT NULL DEFAULT 'manual',
    mapping_note text,
    valid_from date NOT NULL DEFAULT DATE '2000-01-01',
    valid_to date NOT NULL DEFAULT DATE '9999-12-31',
    is_active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE staging.map_apk_cost_object_to_field IS
'Ручная/автоматическая карта соответствий: объект затрат АПК из документов 1С → поле BI';

COMMENT ON COLUMN staging.map_apk_cost_object_to_field.apk_cost_object_key IS
'Значение apk_obekt_zatrat_id / apk_cost_object_key из документа списания материалов на поля';

COMMENT ON COLUMN staging.map_apk_cost_object_to_field.field_sk IS
'Суррогатный ключ поля из mart.dim_field';
