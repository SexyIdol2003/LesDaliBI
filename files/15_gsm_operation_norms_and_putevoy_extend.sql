-- 15_gsm_operation_norms_and_putevoy_extend.sql
-- Подтверждено прямыми запросами OData 2026-09-07:
--   Document_АпкПутевойЛистТракториста -> табличная часть ВыполненныеРаботы (без $expand, приходит инлайн)
--   Catalog_ВидыРаботСотрудников -> табличная часть АпкНормы (нормы ГСМ по модели техники/оборудования, тоже инлайн)
-- Даёт готовый ПЛАН расхода ГСМ по операциям (Вспашка, Опрыскивание, Грядообразование/гребнеобразование)
-- в разрезе моделей техники — именно то, что нужно для показателя "план-факт ГСМ л/га по операциям".

BEGIN;

-- ============================================================
-- 1. Нормы расхода ГСМ по операции + модели техники (ПЛАН)
--    Источник: Catalog_ВидыРаботСотрудников.АпкНормы (нормативно-справочная информация)
-- ============================================================
CREATE TABLE IF NOT EXISTS raw.r1c_operation_fuel_norms (
    operation_id                 uuid NOT NULL,              -- Ref_Key вида работы (= raw.r1c_tech_operations._id)
    line_number                  int NOT NULL,
    model_tehniki_id              uuid,                       -- МодельТехники
    model_oborudovaniya_id         uuid,                       -- МодельОборудования
    smennaya_norma_vyrabotki       numeric(12,3),              -- СменнаяНормаВыработки
    norma_rashoda_topliva           numeric(12,3),              -- НормаРасходаТоплива (план, л на единицу базы)
    prodolzhitelnost_smeny           numeric(6,2),               -- ПродолжительностьСмены, ч
    edinica_smennoy_normy             text,                       -- ЕдиницаИзмеренияСменнойНормыВыработки
    baza_rascheta_gsm                 text,                       -- БазаРасчетаРасходаГСМ ('Гектары' | 'ЕдиницаДопОбъема' | ...)
    klyuch_svyazi_stroki_norm          uuid,                       -- КлючСвязиСтрокиНорм
    _loaded_at                          timestamptz DEFAULT now(),
    PRIMARY KEY (operation_id, line_number)
);

CREATE INDEX IF NOT EXISTS ix_op_fuel_norms_model
    ON raw.r1c_operation_fuel_norms (model_tehniki_id, model_oborudovaniya_id);

-- ============================================================
-- 2. Расширение табличной части путевого листа реальными полями
--    (текущая raw.r1c_putevoy_list_lines хранила только obem_rabot_ga/norma_vyrabotki —
--     этого недостаточно для план-факт по операциям и себестоимости)
-- ============================================================
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS vid_raboty_id uuid;          -- ВидРаботы_Key
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS oborudovanie_id uuid;         -- Оборудование_Key
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS den_raboty date;              -- ДеньРаботы
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS gektarov numeric(12,4);       -- Гектаров (факт площади)
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS tonn numeric(12,4);           -- Тонн
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS kilometrov numeric(12,4);     -- Километров
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS dop_obem_rabot numeric(12,4); -- ДопОбъемРабот
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS edinica_dop_obema_id uuid;    -- ЕдиницаДопОбъема_Key
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS norma_rashoda_gsm numeric(12,4);        -- НормаРасходаГСМ (применённая норма)
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS norma_rashoda_gsm_ed text;              -- НормаРасходаГСМ_Ед
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS normativny_raskhod_gsm numeric(14,4);   -- НормативныйРасходГСМ (план на эту работу, л)
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS chasov numeric(10,3);          -- Часов (факт)
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS chasov_po_norme numeric(10,3); -- ЧасовПоНорме
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS mototochas numeric(10,3);      -- Моточасов
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS osnovnaya_zp numeric(14,2);    -- ОсновнаяЗП
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS itogo_zp numeric(14,2);        -- ИтогоЗП (для себестоимости)
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS tip_zatrat text;                -- ТипЗатрат ('ПрямыеЗатраты' | 'ОбщепроизводственныеЗатраты')
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS analitika_raskhodov_id uuid;    -- АналитикаРасходов (Catalog_СтруктураПредприятия)
ALTER TABLE raw.r1c_putevoy_list_lines ADD COLUMN IF NOT EXISTS klyuch_svyazi uuid;              -- КлючСвязи (связь со строкой норм)

COMMIT;

-- ============================================================
-- 3. Staging: план-факт по операциям и моделям техники (только показатель ГСМ)
-- ============================================================
CREATE OR REPLACE VIEW staging.v_gsm_plan_fact_by_operation AS
SELECT
    l.den_raboty::date                         AS work_date,
    DATE_TRUNC('month', l.den_raboty)::date     AS work_month,
    v.doc_number,
    v.tehnika_id,                                -- техника документа (заголовок путевого листа)
    l.vid_raboty_id,
    COALESCE(op.description, l.vid_raboty_id::text) AS operation_name,
    l.gektarov                                    AS fact_ha,
    l.norma_rashoda_gsm                            AS applied_norm_l_per_unit,
    l.norma_rashoda_gsm_ed                          AS applied_norm_unit,
    l.normativny_raskhod_gsm                         AS plan_liters,     -- ПЛАН: норма x факт объёма (уже посчитан 1С)
    l.dop_obem_rabot,
    l.osnovnaya_zp,
    l.itogo_zp,
    l.tip_zatrat
FROM raw.r1c_putevoy_list_lines l
JOIN raw.r1c_putevoy_list v ON v._id = l.doc_id
LEFT JOIN raw.r1c_tech_operations op ON op._id = l.vid_raboty_id::text
WHERE COALESCE(v._deletionmark, false) = false
  AND COALESCE(v._posted, false) = true;

-- Контроль по трём операциям из запроса:
-- SELECT operation_name, count(*), round(sum(fact_ha),1) AS ga, round(sum(plan_liters),1) AS plan_l
-- FROM staging.v_gsm_plan_fact_by_operation
-- WHERE operation_name ILIKE '%вспашка%' OR operation_name ILIKE '%опрыскивание%' OR operation_name ILIKE '%гребнеобразование%'
-- GROUP BY operation_name ORDER BY plan_l DESC;
