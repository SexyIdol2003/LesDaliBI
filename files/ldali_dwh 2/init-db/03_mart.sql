-- ============================================================================
-- MART-слой: звёздная схема для DataLens / ML
-- Сюда пишет dbt; DataLens и аналитики читают только отсюда.
-- ============================================================================

-- ---- DIM-таблицы ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_day        date PRIMARY KEY,
    year            int NOT NULL,
    quarter         int NOT NULL,
    month           int NOT NULL,
    month_name_ru   text NOT NULL,
    week            int NOT NULL,
    day_of_week     int NOT NULL,
    day_name_ru     text NOT NULL,
    is_weekend      boolean NOT NULL,
    -- Сельхоз-сезон: с 1 октября года N по 30 сентября года N+1.
    -- Старт = «Отчёт о закладке урожая» (до 1 октября) — основа для всех последующих процессов.
    season          int NOT NULL,
    season_label    text NOT NULL,
    -- Фаза агро-цикла: подготовка / посевная / вегетация / уборка
    agro_phase      text,
    -- Фаза продаж: продажи_собственное (1 окт — 14 апр) / продажи_покупное (15 апр — 30 сен)
    sales_phase     text
);
COMMENT ON COLUMN mart.dim_date.season IS 'Сельхоз-сезон: 1 окт N — 30 сен N+1, маркируется годом N (по БП ООО «Лесные дали»).';
COMMENT ON COLUMN mart.dim_date.agro_phase  IS 'Фаза агро-цикла из БП: подготовка | посевная | вегетация | уборка.';
COMMENT ON COLUMN mart.dim_date.sales_phase IS 'Фаза продаж из БП: продажи_собственное | продажи_покупное.';

CREATE TABLE IF NOT EXISTS mart.dim_field (
    field_sk        bigserial PRIMARY KEY,
    field_code_1c   text NOT NULL,                  -- АК-00000031
    field_name      text NOT NULL,                  -- "318", "203", "751" ...
    department      text,
    area_ha         numeric(10,2),                  -- актуальная площадь
    organization    text,
    is_active       boolean NOT NULL DEFAULT true,
    -- SCD Type 2
    valid_from      date NOT NULL DEFAULT CURRENT_DATE,
    valid_to        date NOT NULL DEFAULT '9999-12-31',
    is_current      boolean NOT NULL DEFAULT true,
    UNIQUE (field_code_1c, valid_from)
);
CREATE INDEX IF NOT EXISTS ix_dim_field_current ON mart.dim_field (field_code_1c) WHERE is_current;

CREATE TABLE IF NOT EXISTS mart.dim_crop (
    crop_sk         bigserial PRIMARY KEY,
    variety         text NOT NULL,                  -- Гала / Кармен / Фламинго / Балтик Роуз / Вега / Вэнди / Аметист
    color           text,                           -- Белый / Красный / Фиолетовый
    reproduction    text,                           -- Эб / РС1 / РС2 / РС3 / РС4
    irrigation      text,                           -- полив / без полива / NULL
    UNIQUE (variety, color, reproduction, irrigation)
);
COMMENT ON TABLE mart.dim_crop IS 'Сорт картофеля (нормализованный из артикулов 1С). Уровень аналитики "по сорту".';

CREATE TABLE IF NOT EXISTS mart.dim_nomenclature (
    nom_sk          bigserial PRIMARY KEY,
    article_1c      text,                           -- К_ГПР_ГаБРС3/45+_МСД3НН
    name_1c         text NOT NULL,
    nom_kind        text,                           -- Продукция / Сырье картофель / Материалы* / Услуга ...
    -- Категория верхнего уровня (определяется в staging по правилам)
    category        text NOT NULL,                  -- готовая_продукция / семена / удобрения / СЗР /
                                                    -- ГСМ / тара / упаковка / сырье / урожай / отходы / прочее
    subcategory     text,                           -- мытый / немытый / вакуум / беби / N / P / K / комплексные
    -- Атрибуты, разобранные из артикула (для ГП и сырья картофеля)
    crop_sk         bigint REFERENCES mart.dim_crop(crop_sk),
    caliber         text,                           -- 45+ / 28-45 / некалиброванный / NULL
    pack_type       text,                           -- сетка домик / сетка на рулоне / биг-бэг / вакуум / NULL
    pack_size_kg    numeric(10,3),                  -- 2.5 / 3 / 5 / 25 / 1000 / NULL
    customer_label  text,                           -- ЛЕНТА FRESH / Перекресток / NULL
    is_active       boolean NOT NULL DEFAULT true
);
CREATE INDEX IF NOT EXISTS ix_dim_nom_article  ON mart.dim_nomenclature (article_1c);
CREATE INDEX IF NOT EXISTS ix_dim_nom_category ON mart.dim_nomenclature (category);
CREATE INDEX IF NOT EXISTS ix_dim_nom_crop     ON mart.dim_nomenclature (crop_sk);

CREATE TABLE IF NOT EXISTS mart.dim_equipment (
    eq_sk           bigserial PRIMARY KEY,
    code_1c         text UNIQUE NOT NULL,
    name            text NOT NULL,
    eq_type         text,                           -- трактор / комбайн / погрузчик
    is_active       boolean NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS mart.dim_warehouse (
    wh_sk           bigserial PRIMARY KEY,
    code_1c         text UNIQUE,
    name            text NOT NULL,
    wh_type         text                            -- сырьё / ГП / возвраты / расходники
);

CREATE TABLE IF NOT EXISTS mart.dim_shift (
    shift_sk        bigserial PRIMARY KEY,
    master          text NOT NULL,
    shift_code      text,                           -- день / ночь / сутки
    UNIQUE (master, shift_code)
);

CREATE TABLE IF NOT EXISTS mart.dim_downtime_reason (
    reason_sk       bigserial PRIMARY KEY,
    reason_name     text UNIQUE NOT NULL,
    reason_group    text                            -- оборудование / сырьё / персонал / прочее
);

-- Контрагент-покупатель (по БП — каналы сбыта: сети / HoReCa / опт)
CREATE TABLE IF NOT EXISTS mart.dim_customer (
    customer_sk     bigserial PRIMARY KEY,
    code_1c         text UNIQUE,
    name            text NOT NULL,
    sales_channel   text NOT NULL,                  -- сети / HoReCa / опт / прочее
    network_name    text,                           -- ЛЕНТА / Дикси / Магнит / Перекрёсток / Х5 / ОКЕЙ ...
    is_active       boolean NOT NULL DEFAULT true
);
COMMENT ON TABLE mart.dim_customer IS 'Покупатели и каналы сбыта по БП: сетевые магазины, HoReCa, оптовые покупатели.';

-- ---- FACT-таблицы -----------------------------------------------------------

-- Урожай: одна строка = один выпуск с поля по партии
CREATE TABLE IF NOT EXISTS mart.fact_harvest (
    id              bigserial PRIMARY KEY,
    date_day        date NOT NULL REFERENCES mart.dim_date(date_day),
    field_sk        bigint NOT NULL REFERENCES mart.dim_field(field_sk),
    crop_sk         bigint NOT NULL REFERENCES mart.dim_crop(crop_sk),
    nom_sk          bigint NOT NULL REFERENCES mart.dim_nomenclature(nom_sk),
    plot_no         text,                           -- участок (уч.4, уч.5)
    area_ha         numeric(10,2),                  -- убранная площадь (если меньше площади поля)
    weight_kg       numeric(18,2) NOT NULL,
    yield_t_ha      numeric(10,2) GENERATED ALWAYS AS
                    (CASE WHEN area_ha IS NULL OR area_ha = 0 THEN NULL
                          ELSE (weight_kg / 1000.0) / area_ha END) STORED,
    -- источник
    src_doc_ref     text,
    _loaded_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_harvest_date  ON mart.fact_harvest (date_day);
CREATE INDEX IF NOT EXISTS ix_fact_harvest_field ON mart.fact_harvest (field_sk);
CREATE INDEX IF NOT EXISTS ix_fact_harvest_crop  ON mart.fact_harvest (crop_sk);

-- Затраты на поле в рублях (по операциям из путевых листов / нарядов)
CREATE TABLE IF NOT EXISTS mart.fact_field_costs (
    id              bigserial PRIMARY KEY,
    date_day        date NOT NULL REFERENCES mart.dim_date(date_day),
    field_sk        bigint NOT NULL REFERENCES mart.dim_field(field_sk),
    eq_sk           bigint REFERENCES mart.dim_equipment(eq_sk),
    cost_type       text NOT NULL,                  -- работа техники / зарплата / аренда / услуги / прочее
    operation       text,                           -- вспашка / посадка / опрыскивание / уборка
    hours           numeric(10,2),
    amount_rub      numeric(18,2) NOT NULL,
    src_doc_ref     text,
    _loaded_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_costs_date  ON mart.fact_field_costs (date_day);
CREATE INDEX IF NOT EXISTS ix_fact_costs_field ON mart.fact_field_costs (field_sk);

-- Списания ТМЦ на поля (семена, удобрения, СЗР) — в натуральном и денежном выражении
CREATE TABLE IF NOT EXISTS mart.fact_tmc_usage (
    id              bigserial PRIMARY KEY,
    date_day        date NOT NULL REFERENCES mart.dim_date(date_day),
    field_sk        bigint NOT NULL REFERENCES mart.dim_field(field_sk),
    nom_sk          bigint NOT NULL REFERENCES mart.dim_nomenclature(nom_sk),
    qty             numeric(18,3) NOT NULL,
    uom             text NOT NULL,                  -- кг / л / шт / т
    amount_rub      numeric(18,2),
    src_doc_ref     text,
    _loaded_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_tmc_date  ON mart.fact_tmc_usage (date_day);
CREATE INDEX IF NOT EXISTS ix_fact_tmc_field ON mart.fact_tmc_usage (field_sk);
CREATE INDEX IF NOT EXISTS ix_fact_tmc_nom   ON mart.fact_tmc_usage (nom_sk);

-- ГСМ: расход топлива по технике/полю (из путевых листов)
CREATE TABLE IF NOT EXISTS mart.fact_fuel_usage (
    id              bigserial PRIMARY KEY,
    date_day        date NOT NULL REFERENCES mart.dim_date(date_day),
    eq_sk           bigint NOT NULL REFERENCES mart.dim_equipment(eq_sk),
    field_sk        bigint REFERENCES mart.dim_field(field_sk),
    liters          numeric(12,2) NOT NULL,
    hours           numeric(10,2),
    src_doc_ref     text,
    _loaded_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_fuel_date  ON mart.fact_fuel_usage (date_day);
CREATE INDEX IF NOT EXISTS ix_fact_fuel_eq    ON mart.fact_fuel_usage (eq_sk);

-- Производство: выпуск ГП по сменам (план / факт / производительность)
-- В соответствии с БП различаем источник сырья (свое/покупное) и тип переработки (легкая/глубокая)
CREATE TABLE IF NOT EXISTS mart.fact_production_output (
    id              bigserial PRIMARY KEY,
    date_day        date NOT NULL REFERENCES mart.dim_date(date_day),
    shift_sk        bigint NOT NULL REFERENCES mart.dim_shift(shift_sk),
    nom_sk          bigint REFERENCES mart.dim_nomenclature(nom_sk),
    wh_sk           bigint REFERENCES mart.dim_warehouse(wh_sk),
    -- Атрибуты переработки по БП (процессы 6.1/6.2 — легкая, 7.1/7.2 — глубокая)
    raw_material_source text,                       -- собственное / покупное
    processing_type     text,                       -- легкая / глубокая
    plan_kg         numeric(18,2),
    fact_kg         numeric(18,2),
    plan_hours      numeric(10,2),
    fact_hours      numeric(10,2),
    productivity_kg_h numeric(12,2) GENERATED ALWAYS AS
                    (CASE WHEN fact_hours IS NULL OR fact_hours = 0 THEN NULL
                          ELSE fact_kg / fact_hours END) STORED,
    src_sheet       text,
    _loaded_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_prod_date  ON mart.fact_production_output (date_day);
CREATE INDEX IF NOT EXISTS ix_fact_prod_shift ON mart.fact_production_output (shift_sk);
CREATE INDEX IF NOT EXISTS ix_fact_prod_src   ON mart.fact_production_output (raw_material_source, processing_type);

-- Простои производства
CREATE TABLE IF NOT EXISTS mart.fact_downtime (
    id              bigserial PRIMARY KEY,
    date_day        date NOT NULL REFERENCES mart.dim_date(date_day),
    shift_sk        bigint NOT NULL REFERENCES mart.dim_shift(shift_sk),
    reason_sk       bigint NOT NULL REFERENCES mart.dim_downtime_reason(reason_sk),
    duration_h      numeric(10,2) NOT NULL,
    src_sheet       text,
    _loaded_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_downtime_date ON mart.fact_downtime (date_day);

-- Остатки хранилища (ежедневный snapshot)
CREATE TABLE IF NOT EXISTS mart.fact_storage_balance (
    date_day        date NOT NULL REFERENCES mart.dim_date(date_day),
    wh_sk           bigint NOT NULL REFERENCES mart.dim_warehouse(wh_sk),
    nom_sk          bigint NOT NULL REFERENCES mart.dim_nomenclature(nom_sk),
    qty_kg          numeric(18,2) NOT NULL,
    PRIMARY KEY (date_day, wh_sk, nom_sk)
);
COMMENT ON TABLE mart.fact_storage_balance IS 'Ежедневный snapshot остатков (рассчитывается из движений 1С).';

-- ---- Авто-генерация dim_date на 10 лет вперёд ------------------------------
-- Логика построена строго по документу «Общее описание бизнес-процессов компании»:
--   • Граница сельхоз-сезона — 1 октября (закладка завершена → начинается хранение/продажи).
--     Сезон 2024 = 1 окт 2024 — 30 сен 2025 (включает уборку 2025 года).
--   • Фазы агро-цикла:
--       подготовка    — 1 октября – 31 марта  (подготовка к сезонам, ТО техники, закупки)
--       посевная      — 1 апреля – 31 мая    (1–30 апр — подготовка почвы; 1–31 мая — посев)
--       вегетация     — 1 июня – 14 августа  (полив, СЗР, контроль вегетации, подкормки)
--       уборка        — 15 августа – 30 сентября (сбор + закладка на хранение)
--   • Фаза продаж (sales_phase):
--       продажи_собственное — 1 октября – 14 апреля
--       продажи_покупное    — 15 апреля – 30 сентября

INSERT INTO mart.dim_date (
    date_day, year, quarter, month, month_name_ru, week, day_of_week, day_name_ru,
    is_weekend, season, season_label, agro_phase, sales_phase
)
SELECT
    d::date,
    EXTRACT(YEAR FROM d)::int,
    EXTRACT(QUARTER FROM d)::int,
    EXTRACT(MONTH FROM d)::int,
    (ARRAY['Январь','Февраль','Март','Апрель','Май','Июнь',
           'Июль','Август','Сентябрь','Октябрь','Ноябрь','Декабрь'])[EXTRACT(MONTH FROM d)::int],
    EXTRACT(WEEK FROM d)::int,
    EXTRACT(ISODOW FROM d)::int,
    (ARRAY['Понедельник','Вторник','Среда','Четверг','Пятница','Суббота','Воскресенье'])[EXTRACT(ISODOW FROM d)::int],
    EXTRACT(ISODOW FROM d)::int IN (6,7),
    -- Сезон: новый стартует 1 октября
    CASE WHEN EXTRACT(MONTH FROM d) >= 10
         THEN EXTRACT(YEAR FROM d)::int
         ELSE EXTRACT(YEAR FROM d)::int - 1
    END,
    CASE WHEN EXTRACT(MONTH FROM d) >= 10
         THEN EXTRACT(YEAR FROM d)::int || '-' || (EXTRACT(YEAR FROM d)::int + 1)
         ELSE (EXTRACT(YEAR FROM d)::int - 1) || '-' || EXTRACT(YEAR FROM d)::int
    END,
    -- Агро-фаза: учитываем точные границы из БП (включая 15 августа)
    CASE
        WHEN EXTRACT(MONTH FROM d)::int IN (10, 11, 12, 1, 2, 3) THEN 'подготовка'
        WHEN EXTRACT(MONTH FROM d)::int IN (4, 5)                THEN 'посевная'
        WHEN EXTRACT(MONTH FROM d)::int IN (6, 7)                THEN 'вегетация'
        WHEN EXTRACT(MONTH FROM d)::int = 8 AND EXTRACT(DAY FROM d)::int < 15  THEN 'вегетация'
        WHEN EXTRACT(MONTH FROM d)::int = 8 AND EXTRACT(DAY FROM d)::int >= 15 THEN 'уборка'
        WHEN EXTRACT(MONTH FROM d)::int = 9                      THEN 'уборка'
    END,
    -- Фаза продаж: с 15 апреля по 30 сентября — покупное, остальное — собственное
    CASE
        WHEN EXTRACT(MONTH FROM d)::int = 4 AND EXTRACT(DAY FROM d)::int >= 15 THEN 'продажи_покупное'
        WHEN EXTRACT(MONTH FROM d)::int IN (5, 6, 7, 8, 9)                     THEN 'продажи_покупное'
        ELSE 'продажи_собственное'
    END
FROM generate_series('2020-01-01'::date, '2030-12-31'::date, interval '1 day') d
ON CONFLICT (date_day) DO NOTHING;
