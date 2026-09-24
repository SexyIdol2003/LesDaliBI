# LesDaliBI — подробный отчёт по сессии 24.09.2026

> **Статус документа:** составлен только по терминальному выводу и SQL, показанным в этой сессии.
>  
> **Принцип:** где результат не был показан или действие не было подтверждено выводом, это прямо отмечено как «не подтверждено».

---

## Цель сессии

Целью работы было подготовить проверяемый фундамент для KPI №6 — прямых затрат на поле / на гектар. В сессии последовательно дорабатывались четыре слоя:

1. Экономическая площадь поля на дату.
2. Трудовые затраты по полевым операциям.
3. Фактическое использование семян, удобрений и СЗР на поле.
4. Строгая денежная оценка этих агровходов по закупочным ценам.

Локальный репозиторий:

```text
~/Projects/LesDaliBI
```

DWH запускается через:

```text
files/docker-compose.yml
```

PostgreSQL:

```text
postgres-dwh
-U ldali_admin
-d ldali_dwh
```

Для DDL-smoke test использовалась схема:

```text
BEGIN;
-- CREATE OR REPLACE VIEW / COMMENT / GRANT
-- QA-запросы
ROLLBACK;
```

---

# Экономическая площадь поля

## Изменённый файл

```text
files/52_mart_field_economic_area_by_date.sql
```

Основная витрина:

```text
mart.v_field_economic_area_by_date
```

Дополнительный объект:

```text
mart.v_field_economic_area_by_date_quality
```

Назначение — давать экономическую площадь конкретного поля на конкретную дату для дальнейшего присоединения операций, труда и прямых затрат.

## Контракт витрины

| № | Поле | Тип |
|---:|---|---|
| 1 | `snapshot_date` | `date` |
| 2 | `season` | `integer` |
| 3 | `pole_id` | `text` |
| 4 | `field_name` | `text` |
| 5 | `active_contours_count` | `bigint` |
| 6 | `active_crops_count` | `bigint` |
| 7 | `economic_area_ha` | `numeric` |
| 8 | `max_single_contour_area_ha` | `numeric` |
| 9 | `min_active_period_start` | `date` |
| 10 | `max_active_period_end` | `date` |
| 11 | `active_crops_and_areas` | `text` |
| 12 | `history_row_ids` | `text[]` |
| 13 | `economic_area_source` | `text` |
| 14 | `economic_area_method` | `text` |
| 15 | `economic_area_quality_status` | `text` |
| 16 | `calculated_at` | `timestamp with time zone` |

## Проверка метода расчёта площади

| Сезон | Источник | Метод | Строк field-date | Поля | Мин. площадь, га | Макс. площадь, га | Не-OK строк |
|---:|---|---|---:|---:|---:|---:|---:|
| 2021 | `ARCHIVED_HISTORY_FIELD_CONTOURS` | `ARCHIVED_ACTIVE_CONTOURS_SUM_ON_DATE` | 12 569 | 39 | 3.00 | 569.60 | 0 |
| 2022 | `ARCHIVED_HISTORY_FIELD_CONTOURS` | `ARCHIVED_ACTIVE_CONTOURS_SUM_ON_DATE` | 13 842 | 39 | 3.00 | 930.00 | 0 |
| 2023 | `ARCHIVED_HISTORY_FIELD_CONTOURS` | `ARCHIVED_ACTIVE_CONTOURS_SUM_ON_DATE` | 14 600 | 40 | 0.99 | 465.04 | 0 |
| 2024 | `ARCHIVED_HISTORY_FIELD_CONTOURS` | `ARCHIVED_ACTIVE_CONTOURS_SUM_ON_DATE` | 14 735 | 48 | 0.99 | 465.04 | 0 |
| 2025 | `ARCHIVED_HISTORY_FIELD_CONTOURS` | `ARCHIVED_ACTIVE_CONTOURS_SUM_ON_DATE` | 17 155 | 47 | 2.29 | 535.58 | 0 |
| 2026 | `ACTIVE_HISTORY_FIELD_CONTOURS` | `ACTIVE_CONTOURS_SUM_ON_DATE` | 17 155 | 47 | 2.29 | 465.04 | 0 |

Проверка неожиданных комбинаций `season × source × method` вернула 0 строк.

## Подтверждённое развёртывание

`52_mart_field_economic_area_by_date.sql` был применён вне транзакции.

Post-deploy QA:

```text
Всего field-date строк: 90 056
Диапазон snapshot_date: 2021-01-01 — 2026-12-31
Distinct fields: 48
```

Зависимая витрина `mart.v_field_day_operation_work_coverage` осталась доступной:

```text
1 193 строк
min(work_date): 2026-05-05
max(work_date): 2026-08-31
```

## Покрытие работ экономической площадью

Соединение проверялось по:

```text
pole_id + work_date = pole_id + snapshot_date
```

| Год | Строк работ с полем | Покрыто | Не покрыто | Покрытие строк | Рабочие га | Покрытые га | Непокрытые га | Покрытие га |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2021 | 1 825 | 1 716 | 109 | 94.03% | 5 686.45 | 4 686.35 | 1 000.10 | 82.41% |
| 2022 | 2 329 | 2 326 | 3 | 99.87% | 11 247.26 | 11 243.26 | 4.00 | 99.96% |
| 2023 | 2 027 | 2 027 | 0 | 100.00% | 14 767.96 | 14 767.96 | 0.00 | 100.00% |
| 2024 | 1 137 | 1 137 | 0 | 100.00% | 7 764.80 | 7 764.80 | 0.00 | 100.00% |
| 2025 | 2 299 | 2 299 | 0 | 100.00% | 14 322.30 | 14 322.30 | 0.00 | 100.00% |
| 2026 | 1 536 | 1 536 | 0 | 100.00% | 14 688.12 | 14 688.12 | 0.00 | 100.00% |

Вывод:

- 2023–2026 покрыты на 100%.
- Непокрытые операции 2021–2022 не удалены.
- В KPI на гектар необходимо использовать статус покрытия площади.

---

# Трудовые затраты на поле

## Созданный файл

```text
files/55_mart_field_day_operation_labor_cost.sql
```

Витрина:

```text
mart.v_field_day_operation_labor_cost
```

Она агрегирует фактический труд из путевых листов на уровне:

```text
дата × поле × операция × вид работы × техника × тип затрат
```

Источник трудовой суммы:

```text
staging.stg_waybill_field_link.labor_amount_rub
```

В витрину включаются строки:

```text
link_status = 'resolved_one_field_active'
```

## Существенная логика

Соединение с экономической площадью изменено с:

```sql
JOIN mart.v_field_economic_area_by_date
```

на:

```sql
LEFT JOIN mart.v_field_economic_area_by_date
```

Это сохраняет строки работ 2021–2022 без найденной площади на дату и маркирует их:

```text
NO_ECONOMIC_AREA_ON_WORK_DATE
```

Статусы труда:

```text
OK_LABOR_AND_WORK_HA
LABOR_WITHOUT_WORK_HA
WORK_HA_WITHOUT_LABOR
NO_LABOR_AND_NO_WORK_HA
```

Источник труда:

```text
ACTUAL_LABOR_FROM_WAYBILL
```

## QA-значения по годам

| Год | Всего labour, ₽ | Labour с площадью, ₽ | Рабочие га с площадью | Покрытие рабочих га |
|---:|---:|---:|---:|---:|
| 2021 | 2 874 764.45 | 1 577 208.85 | 4 686.35 | 82.41% |
| 2022 | 7 054 203.42 | 2 526 754.70 | 11 243.26 | 99.96% |
| 2023 | 8 779 927.58 | 3 482 974.19 | 14 767.96 | 100.00% |
| 2024 | 6 390 653.83 | 2 486 193.56 | 7 764.80 | 100.00% |
| 2025 | 8 543 897.13 | 5 679 067.91 | 14 322.30 | 100.00% |
| 2026 | 6 807 469.62 | 3 931 494.52 | 14 688.12 | 100.00% |

Статус: файл подготовлен и добавлен в Git index. В полном доступном логе нет отдельного подтверждённого production deploy `55` с post-deploy QA; перед следующим этапом его нужно перепроверить.

---

# Фактическое использование агровходов

## Созданный и применённый файл

```text
files/57_mart_field_agro_input_usage_direct.sql
```

Витрина:

```text
mart.v_field_agro_input_usage_direct
```

Гранулярность:

```text
документ списания × строка документа × поле × номенклатура
```

Источники:

```text
raw.r1c_field_material_writeoff_doc
raw.r1c_field_material_writeoff_lines
staging.map_apk_cost_object_to_field
mart.dim_field
raw.r1c_nomenclature
```

Категории:

```text
SEEDS
FERTILIZERS
CROP_PROTECTION
UNMATCHED_NOMENCLATURE
OTHER_AGRO_INPUT
```

Ключевые статусы:

```text
OK_CLASSIFIED_AGRO_INPUT
NO_NOMENCLATURE_MATCH
NO_FIELD_MAPPING
UNCLASSIFIED_OR_OTHER_NOMENCLATURE
```

## Post-deploy QA

| Метрика | Значение |
|---|---:|
| Строк использования | 4 861 |
| Уникальные документы | 1 854 |
| Поля | 23 |
| Минимальная дата | 2021-07-01 |
| Максимальная дата | 2026-08-27 |
| Классифицировано | 4 544 |
| Несопоставленные номенклатуры | 317 |
| Строк без документной стоимости | 4 861 |
| `datalens_ro` имеет `SELECT` | `true` |

| Статус | Строки | Номенклатуры | Поля | Количество |
|---|---:|---:|---:|---:|
| `OK_CLASSIFIED_AGRO_INPUT` | 4 544 | 133 | 17 | 3 625 035.8810 |
| `NO_NOMENCLATURE_MATCH` | 317 | 22 | 23 | 3 612 797.8520 |

Все строки имеют:

```text
NO_DOCUMENT_COST
```

Следствие: суммы из документов списания нельзя использовать как источник денежной оценки.

---

# Строгая стоимость агровходов

## Подготовленный файл

```text
files/58_mart_field_agro_input_cost_direct.sql
```

Целевая витрина:

```text
mart.v_field_agro_input_cost_direct
```

Витрина включает:

```text
SEEDS
FERTILIZERS
CROP_PROTECTION
```

и поэтому должна содержать 4 544 классифицированные строки, без 317 строк `UNMATCHED_NOMENCLATURE`.

Источник закупочных цен:

```text
mart.v_agro_input_purchase_price
```

Сопоставление:

```text
pp.nomenklatura_key = u.nomenklatura_id
pp.data_quality_status = 'OK'
```

Выбор цены:

```sql
ORDER BY
    (pp.document_date <= u.document_date) DESC,
    ABS(pp.document_date - u.document_date) ASC
LIMIT 1
```

Статусы:

```text
PURCHASE_PRICE_ON_OR_BEFORE_WRITEOFF
ONLY_PRICE_AFTER_WRITEOFF
NO_PURCHASE_PRICE
```

## Денежные меры

```text
diagnostic_estimated_cost_rub_no_vat
```

Это количество × найденная цена. Может использовать цену после списания — только диагностика.

```text
strict_cost_rub_no_vat
```

Это количество × цена только когда:

```text
price_document_date <= document_date
```

Если строгой цены нет, значение — `NULL`, а не `0`.

## Результаты smoke test

Smoke test был выполнен в транзакции и завершился:

```text
ROLLBACK
```

Следовательно, контракт и расчёт были протестированы, но финальный production deploy `58` в показанном логе не подтверждён.

### Распределение качества

| Статус | Строки | Номенклатуры | Количество | Strict cost, ₽ |
|---|---:|---:|---:|---:|
| `OK_STRICT_COST` | 4 223 | 125 | 1 570 272.5260 | 178 179 553.68 |
| `PRICE_AFTER_WRITEOFF_NOT_IN_STRICT_COST` | 291 | 13 | 1 817.3300 | NULL |
| `NO_PURCHASE_PRICE` | 30 | 8 | 2 052 946.0250 | NULL |

### Строгая стоимость по годам и категориям

| Год | Категория | Строк | Со strict cost | Без strict cost | Strict cost, ₽ без НДС |
|---:|---|---:|---:|---:|---:|
| 2021 | CROP_PROTECTION | 11 | 7 | 4 | 132 141.74 |
| 2021 | FERTILIZERS | 14 | 14 | 0 | 5 603 288.00 |
| 2022 | CROP_PROTECTION | 1 018 | 834 | 184 | 15 963 411.10 |
| 2022 | FERTILIZERS | 289 | 289 | 0 | 10 211 125.02 |
| 2022 | SEEDS | 1 | 1 | 0 | 180 000.00 |
| 2023 | CROP_PROTECTION | 1 329 | 1 292 | 37 | 20 304 299.12 |
| 2023 | FERTILIZERS | 471 | 395 | 76 | 21 416 630.58 |
| 2024 | CROP_PROTECTION | 408 | 408 | 0 | 9 024 818.40 |
| 2024 | FERTILIZERS | 96 | 96 | 0 | 10 224 907.37 |
| 2025 | CROP_PROTECTION | 373 | 373 | 0 | 13 784 906.12 |
| 2025 | FERTILIZERS | 54 | 54 | 0 | 16 836 793.30 |
| 2025 | SEEDS | 8 | 0 | 8 | NULL |
| 2026 | CROP_PROTECTION | 398 | 395 | 3 | 25 800 746.00 |
| 2026 | FERTILIZERS | 65 | 65 | 0 | 28 696 486.93 |
| 2026 | SEEDS | 9 | 0 | 9 | NULL |

---

# Расследование стоимости собственных семян

## Положительный контроль: купленные семена

Найдена и успешно оценена номенклатура:

```text
Семена Любаша РС1
nomenklatura_id: a2cbedfa-86bf-11ed-9c4c-00505689ff45
```

| Показатель | Значение |
|---|---|
| Строк списания | 1 |
| Дата списания | 2022-10-14 |
| Количество | 1 000 |
| Строк закупочной цены | 1 |
| Дата закупки | 2022-10-14 |
| Цена без НДС | 180.0000 ₽/ед. |
| Статус закупочной цены | `OK` |
| Итог | `PURCHASE_KEY_AND_PRICE_PRESENT` |

Это подтверждает работоспособность прямой связки:

```text
списание → nomenklatura_id → purchase-price view
```

для номенклатуры, купленной под тем же ключом.

## Собственные семена без прямой закупочной цены

| Год | Номенклатура | `nomenklatura_id` | Строк | Количество |
|---:|---|---|---:|---:|
| 2025 | Семена собств ГалаРС2 Белый | `1360869d-2fe0-11f0-b80e-005056bde474` | 7 | 658 839.0000 |
| 2025 | Семена собств ФламингоРС2 Красный | `4a57271b-2fe0-11f0-b80e-005056bde474` | 1 | 184 006.0000 |
| 2026 | Семена собств ГалаРС1 Белый | `a6c841eb-2fdf-11f0-b80e-005056bde474` | 5 | 781 640.0000 |
| 2026 | Семена собств ГалаРС3 Белый | `d1c07196-4922-11f1-b821-005056bde474` | 3 | 356 701.0000 |
| 2026 | Семена собств КарменРС1 Красный | `42393b1f-8977-11f0-b818-005056bde474` | 1 | 70 326.0000 |

Итого:

```text
17 строк
2 052 946.0000 единиц
```

## Что проверено

### 1. Закупки

Источник:

```text
mart.v_agro_input_purchase_price
```

Для всех пяти GUID результат:

```text
NO_KEY_MATCH_IN_PURCHASE_PRICE_VIEW
```

Это подтверждает отсутствие прямого совпадения внутренней номенклатуры собственных семян с закупочной номенклатурой. Это не доказывает отсутствие исторических покупок исходных семян с другими GUID.

### 2. Выпуск урожая

Источник:

```text
mart.fact_vypusk_urozhaya
```

Контракт содержит:

```text
doc_id
doc_number
doc_date
pole_id
nomenklatura_id
edinica_id
kolichestvo
summa
seriya
_updated_at
line_number
```

По пяти GUID собственных семян:

```text
0 строк
NO_OUTPUT_RECORD
```

В текущем выпуске урожая нет строк этих номенклатур.

### 3. Движение продукции

Цепочка:

```text
raw.r1c_dvizhenie_produkcii
raw.r1c_dvizhenie_produkcii_lines
staging.v_dvizhenie_produkcii_clean
staging.v_dvizhenie_produkcii_lines_clean
mart.v_tmc_movement
```

По всем 17 строкам найдено:

```text
operation_name = ПередачаМатериаловВКладовую
movement_type  = material_return_to_warehouse
source_amount = 0.00
source_amount_signed = 0.00
unit_cost_rub = 0.000000
seriya = NULL/пусто
field_mapping_status = Сопоставлено
```

Найденные движения связаны с полями 318, 201 и 751.

В исходной raw-таблице строк движения единственным денежным полем является:

```text
raw.r1c_dvizhenie_produkcii_lines.summa
```

Оно транслируется в:

```text
staging.v_dvizhenie_produkcii_lines_clean.summa
→ mart.v_tmc_movement.source_amount
```

Следовательно, нулевая стоимость в mart не возникает из-за потери в трансформации: в проверенном контуре движения продукции нет ненулевой суммы.

Это не доказывает, что себестоимость отсутствует в 1С. Это означает, что в проверенных выгрузках не найдено денежного значения для этих движений.

---

# Что остаётся сделать

## 1. Доработать и применить `58`

В `files/58_mart_field_agro_input_cost_direct.sql` рекомендуется добавить отдельный статус собственных семян до общего `NO_PURCHASE_PRICE`:

```sql
WHEN agro_input_category = 'SEEDS'
 AND lower(COALESCE(nomenklatura_name, '')) LIKE 'семена собств%'
    THEN 'OWN_SEED_COST_NOT_AVAILABLE_IN_EXTRACT'
```

После правки ожидается:

| Статус | Ожидаемые строки |
|---|---:|
| `OK_STRICT_COST` | 4 223 |
| `PRICE_AFTER_WRITEOFF_NOT_IN_STRICT_COST` | 291 |
| `OWN_SEED_COST_NOT_AVAILABLE_IN_EXTRACT` | 17 |
| `NO_PURCHASE_PRICE` | 13 |

После smoke test нужно применить файл:

```bash
cd ~/Projects/LesDaliBI || exit 1

docker compose \
  -f files/docker-compose.yml \
  exec -T postgres-dwh \
  psql \
    -v ON_ERROR_STOP=1 \
    -U ldali_admin \
    -d ldali_dwh \
    -f /dev/stdin \
  < files/58_mart_field_agro_input_cost_direct.sql
```

После применения проверить:

```sql
SELECT
    agro_cost_quality_status,
    COUNT(*) AS material_lines,
    COUNT(DISTINCT nomenklatura_id) AS nomenclatures_count,
    ROUND(SUM(COALESCE(quantity, 0)), 4) AS quantity,
    ROUND(SUM(strict_cost_rub_no_vat), 2) AS strict_cost_rub_no_vat
FROM mart.v_field_agro_input_cost_direct
GROUP BY agro_cost_quality_status
ORDER BY material_lines DESC, agro_cost_quality_status;

SELECT has_table_privilege(
    'datalens_ro',
    'mart.v_field_agro_input_cost_direct',
    'SELECT'
) AS datalens_ro_has_select;
```

## 2. Повторно подтвердить `55`

Нужно:

1. Проверить актуальный diff `files/55_mart_field_day_operation_labor_cost.sql`.
2. Повторить транзакционный smoke test.
3. При необходимости применить файл вне транзакции.
4. Проверить доступ `datalens_ro`.

## 3. Искать происхождение семян через существующий OData URL

Следующий поиск должен идти через тот же base URL OData, который уже используется проектом.

Нужно:

1. посмотреть точные entity set в `$metadata`;
2. найти entity set справочника партнёров;
3. связать поставщиков с `ПриобретениеТоваровУслуг`;
4. искать исторические покупки и номенклатуру по:

```text
Гала
Кармен
Фламинго
Любаша
РС1
РС2
РС3
семена
```

Цена исторической покупки исходного семени не должна автоматически использоваться как strict cost собственного семени. Такая цена может быть только диагностической, пока не найден источник фактической себестоимости собственного производства.

## 4. Найти источник фактической себестоимости собственных семян

Нужен источник 1С, содержащий минимум:

```text
period_date
organization_id
nomenklatura_id
characteristic_id
series_id / seriya
warehouse_id
quantity
cost_rub / amount_rub
unit_cost_rub
source_document_id
calculation_status
```

Желаемая гранулярность:

```text
период/дата расчёта × организация × номенклатура × характеристика × серия × склад
```

## 5. Построить KPI №6

Ориентировочная итоговая витрина:

```text
mart.v_field_month_kpi6_direct_cost
```

Минимальные компоненты:

```text
period_month
season_year
field_sk
pole_id
field_name
economic_area_ha
labor_cost_rub
allocated_fuel_liters
strict_fuel_cost_rub
seeds_strict_cost_rub
fertilizers_strict_cost_rub
crop_protection_strict_cost_rub
total_strict_direct_cost_rub
strict_direct_cost_rub_per_economic_ha
cost_coverage_status
```

Возможные статусы покрытия:

```text
FULL_STRICT_COVERAGE
PARTIAL_AGRO_PRICE_COVERAGE
OWN_SEED_COST_NOT_AVAILABLE_IN_EXTRACT
NO_FUEL_PRICE_COVERAGE
NO_ECONOMIC_AREA
```

---

# Git-статус и фиксация

На момент проверки в index были:

```text
files/52_mart_field_economic_area_by_date.sql
files/55_mart_field_day_operation_labor_cost.sql
files/57_mart_field_agro_input_usage_direct.sql
files/58_mart_field_agro_input_cost_direct.sql
```

Проверка:

```text
git diff --cached --check
```

не вывела ошибок.

Зафиксированные правила:

1. Не заменять неизвестную стоимость нулём.
2. Не включать цену после даты списания в strict cost.
3. Не подставлять автоматически цену исходного покупного семени как стоимость собственного семени.
4. Сохранять физический расход при отсутствии денежного покрытия.
5. Не терять трудовые операции без площади на дату.
6. Показывать статусы покрытия в BI.
7. Работать с 1С через фактические OData entity set и существующий base URL.

## Краткий итог

За сессию применён слой экономической площади и опубликована DataLens-доступная витрина фактического field-level использования агровходов. Подготовлена и подробно протестирована витрина строгой стоимости семян, удобрений и СЗР по закупочным ценам, но её финальное production-применение в показанном логе не подтверждено. Для собственных семян 2025–2026 подтверждены физические списания и движения на поля, но в проверенных выгрузках закупок, выпуска урожая и движения продукции не найдена денежная стоимость. Следующий шаг — завершить и применить `58`, подтвердить `55`, продолжить поиск через существующий OData endpoint и найти отдельный источник фактической себестоимости собственных семян.
