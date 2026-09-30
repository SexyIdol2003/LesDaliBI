# Сессия 2026-09-30 — KPI5: распределение списания ГСМ по полям и сценарная закупочная цена

## Статус

Сессия завершена успешно.

В DWH создано представление:

```sql
mart.v_kpi5_fuel_field_allocation_scenario
```

В репозитории созданы и локально закоммичены:

- `files/75_mart_kpi5_fuel_field_allocation_scenario.sql`
- `files/75_mart_kpi5_fuel_field_allocation_scenario_qa.sql`

QA выполнен успешно:

| Проверка | Результат |
|---|---:|
| Расхождения по ключам `(period_month, equipment_sk, fuel_brand_id)` | `0` |
| Строки `FIELD_ALLOCATED` без `pole_id` | `0` |
| Строки `NOT_ALLOCATED_TO_FIELD` с заполненным `pole_id` | `0` |
| Исходное списание ГСМ | `637470.1180` л |
| Выход представления | `637470.1180000000000002323749148627` л |
| Допуск для сравнения | `0.000001` л |
| Итог | QA passed |

Незначительный длинный десятичный хвост у выходной суммы является вычислительной погрешностью `numeric`-операций и находится в пределах принятого допуска. Потери топлива не обнаружены.

---

## Цель

Реализовать сценарное представление KPI5, которое:

1. Берёт списанные литры ГСМ из `mart.v_kpi5_fuel_purchase_price_scenario`.
2. Распределяет их по подтверждённо связанным полям и агрооперациям из `mart.v_field_fuel_cost_allocated_exact_brand`.
3. Не допускает распределения по полям выше доступного объёма списания по ключу:
   - месяц;
   - единица техники;
   - марка топлива.
4. Сохраняет нераспределённый остаток отдельной строкой.
5. Сохраняет сценарный метод и параметры закупочной цены без НДС.
6. Не выдаёт результат за фактическую бухгалтерскую себестоимость.
7. Даёт DataLens-доступ через роль `datalens_ro`.

---

## Созданное представление

Файл:

```text
files/75_mart_kpi5_fuel_field_allocation_scenario.sql
```

Представление:

```sql
mart.v_kpi5_fuel_field_allocation_scenario
```

### Зерно результата

Строка результата описывает сценарную оценку списания топлива на уровне:

```text
period_month
season_year
equipment_sk
equipment_name
fuel_brand_id
pole_id
agr_operaciya_id
allocation_status
price_method
price_month
price_age_months
price_no_vat_rub_per_liter
```

### Поля результата

| Поле | Значение |
|---|---|
| `period_month` | Месяц списания |
| `season_year` | Сезон |
| `equipment_sk` | Суррогатный ключ техники |
| `equipment_name` | Наименование техники |
| `fuel_brand_id` | Идентификатор марки топлива |
| `pole_id` | Поле, если литры подтверждённо распределены |
| `agr_operaciya_id` | Агрооперация, если литры подтверждённо распределены |
| `allocation_status` | `FIELD_ALLOCATED` или `NOT_ALLOCATED_TO_FIELD` |
| `price_method` | Метод подбора закупочной цены |
| `price_month` | Месяц найденной цены |
| `price_age_months` | Возраст цены в месяцах |
| `price_no_vat_rub_per_liter` | Цена без НДС за литр |
| `liters` | Сценарные литры в строке |
| `estimated_fuel_cost_no_vat_rub` | `liters * price_no_vat_rub_per_liter` |

### Статусы распределения

#### `FIELD_ALLOCATED`

Литры привязаны к полю (`pole_id`) и агрооперации (`agr_operaciya_id`).

Источник полевой детализации:

```sql
mart.v_field_fuel_cost_allocated_exact_brand
```

Распределение делается только для строк, где:

```sql
allocated_brand_field_liters > 0
```

#### `NOT_ALLOCATED_TO_FIELD`

Остаток списания по ключу:

```text
period_month + equipment_sk + fuel_brand_id
```

который нельзя подтверждённо распределить по полям.

Для таких строк `pole_id` и `agr_operaciya_id` должны быть `NULL`.

---

## Алгоритм распределения

В представлении используются три CTE.

### 1. `field_rows`

Получает полевые строки распределения топлива и рассчитывает доступный полевой объём на ключ:

```sql
SUM(f.allocated_brand_field_liters) OVER (
    PARTITION BY f.month_start, f.eq_sk, f.fuel_brand_id
) AS all_field_liters
```

То есть рассчитывается общий объём топливной детализации по:

```text
month_start + eq_sk + fuel_brand_id
```

### 2. `joined`

Соединяет сценарные списания с полевыми строками по:

```sql
f.month_start = s.period_month
AND f.eq_sk = s.equipment_sk
AND f.fuel_brand_id = s.fuel_brand_id
```

Для каждой полевой строки рассчитывается:

```sql
f.allocated_brand_field_liters
* LEAST(
    1::numeric,
    s.writeoff_liters / NULLIF(f.all_field_liters, 0)
)
```

Следствия:

- если суммарные полевые литры меньше либо равны списанию, поле получает полный подтверждённый объём;
- если полевые литры больше списания, полевые строки пропорционально уменьшаются;
- объём `FIELD_ALLOCATED` не превышает `writeoff_liters`;
- защита от деления на ноль реализована через `NULLIF(..., 0)`.

### 3. `output_rows`

Формирует две группы строк:

1. `FIELD_ALLOCATED` — строки, где `field_liters > 0`;
2. `NOT_ALLOCATED_TO_FIELD` — остаток:

```sql
GREATEST(
    MAX(writeoff_liters) - COALESCE(SUM(field_liters), 0),
    0
)
```

Нулевые остатки не выводятся.

---

## Семантическое ограничение

Представление является **сценарной оценкой** стоимости и распределения топлива:

- закупочная цена берётся из сценария `v_kpi5_fuel_purchase_price_scenario`;
- цена указана без НДС;
- это не фактическая учётная себестоимость;
- это не замена KPI5/KPI6;
- это не является бухгалтерской проводкой;
- нераспределённый остаток сохраняется явно и не принудительно распределяется по полям.

Комментарий на представлении:

```text
Оценка ГСМ по технике, месяцу, марке и подтверждённо распределённому полю;
неприписанные полям литры сохранены отдельно. Цена закупочная без НДС,
сценарий прошлой закупки помечен. Не фактическая учётная себестоимость
и не замена KPI5/KPI6.
```

---

## Применение в БД

Представление было применено в контейнер PostgreSQL:

```text
ldali-postgres-dwh
```

Подключение выполнялось к:

```text
database: ldali_dwh
user: ldali_admin
```

При применении был получен успешный вывод:

```text
BEGIN
CREATE VIEW
COMMENT
GRANT
COMMIT
```

Это означает:

- транзакция началась успешно;
- представление создано;
- комментарий добавлен;
- роль `datalens_ro` получила `SELECT`;
- транзакция подтверждена.

Повторно выполнять миграцию №75 в этой же БД не нужно, так как объект уже создан.

---

## Отмена тяжёлых запросов

Во время сессии случайно были запущены два тяжёлых диагностических запроса с CTE `WITH source AS (...)`, которые работали несколько минут.

Были обнаружены PID:

```text
57485
57643
```

Оба запроса были отменены безопасно через:

```sql
SELECT pid, pg_cancel_backend(pid) AS cancel_sent
FROM pg_stat_activity
WHERE pid IN (57485, 57643)
  AND state = 'active'
  AND query LIKE 'WITH source AS (%';
```

Результат:

```text
57485 | t
57643 | t
```

Последующая проверка:

```sql
SELECT pid, state, now() - query_start AS running_for
FROM pg_stat_activity
WHERE pid IN (57485, 57643);
```

вернула:

```text
(0 rows)
```

То есть оба активных сеанса завершились.

В одной из рабочих вкладок было получено ожидаемое сообщение:

```text
ERROR: canceling statement due to user request
```

Это относится только к отменённому `SELECT`. Данные не были удалены и PostgreSQL не был остановлен.

---

## Почему исходный QA был тяжёлым

В первоначальной версии файла №75 блок `DO $check$` отдельно обращался к тяжёлым представлениям несколько раз:

1. проверка баланса по ключам;
2. проверка `FIELD_ALLOCATED` без `pole_id`;
3. проверка `NOT_ALLOCATED_TO_FIELD` с заполненным `pole_id`;
4. расчёт общей суммы источника;
5. расчёт общей суммы результата;
6. отдельная итоговая агрегация после `COMMIT`.

Обычное PostgreSQL `VIEW` не хранит физический результат: запрос представления выполняется заново при каждом обращении. Поэтому многократные обращения к №74 и №75 приводят к повторному расчёту тяжёлой цепочки.

---

## Разделение DDL и QA

Чтобы миграция №75 не содержала тяжёлую валидацию и отчёт, код был разделён на два файла.

### Миграция

```text
files/75_mart_kpi5_fuel_field_allocation_scenario.sql
```

Содержит только:

- `BEGIN`;
- `CREATE VIEW`;
- `COMMENT ON VIEW`;
- `GRANT SELECT`;
- `COMMIT`.

### QA

```text
files/75_mart_kpi5_fuel_field_allocation_scenario_qa.sql
```

Содержит отдельный запускаемый QA-запрос.

QA не является миграцией. Его следует выполнять отдельно после изменения логики, обновления исходных представлений либо перед публикацией витрины в DataLens.

---

## Исправление QA для нулевых источников

Первичная сверка по ключам показала 10 строк с:

```text
source_liters = 0.0000
output_liters = NULL
```

Примеры:

| period_month | equipment_sk | fuel_brand_id |
|---|---:|---|
| 2021-07-01 | 166 | `9b060c6d-71d9-11e7-85a2-d43d7eec442d` |
| 2022-02-01 | 163 | `9b060c6d-71d9-11e7-85a2-d43d7eec442d` |
| 2022-03-01 | 163 | `9b060c6d-71d9-11e7-85a2-d43d7eec442d` |
| 2022-04-01 | 155 | `9b060c6d-71d9-11e7-85a2-d43d7eec442d` |
| 2022-04-01 | 156 | `9b060c6d-71d9-11e7-85a2-d43d7eec442d` |
| 2022-04-01 | 164 | `9b060c6d-71d9-11e7-85a2-d43d7eec442d` |
| 2022-04-01 | 166 | `9b060c6d-71d9-11e7-85a2-d43d7eec442d` |
| 2022-05-01 | 159 | `9b060c6d-71d9-11e7-85a2-d43d7eec442d` |
| 2022-05-01 | 166 | `9b060c6d-71d9-11e7-85a2-d43d7eec442d` |
| 2022-06-01 | 163 | `9b060c6d-71d9-11e7-85a2-d43d7eec442d` |

Это не потеря данных. В №75 нулевые результаты не выводятся намеренно:

```sql
WHERE field_liters > 0
```

и:

```sql
HAVING GREATEST(
    MAX(writeoff_liters) - COALESCE(SUM(field_liters), 0),
    0
) > 0
```

Поэтому отсутствие строки результата корректно для ключа с `source_liters = 0`.

Условие QA было исправлено с:

```sql
WHERE source_liters IS NULL
   OR output_liters IS NULL
   OR ABS(source_liters - output_liters) > 0.000001
```

на:

```sql
WHERE source_liters IS NULL
   OR ABS(source_liters - COALESCE(output_liters, 0)) > 0.000001
```

Это сохраняет обнаружение реальных расхождений, но не считает ошибкой отсутствие выходной строки для нулевого источника.

---

## Финальная версия QA

Файл:

```text
files/75_mart_kpi5_fuel_field_allocation_scenario_qa.sql
```

Использует:

```sql
SET statement_timeout = '120s';
```

и единый материализованный CTE:

```sql
WITH rows AS MATERIALIZED (...)
```

В CTE объединяются:

- источник №74 с `origin = 'source'`;
- результат №75 с `origin = 'output'`.

Затем выполняется агрегация по ключу:

```text
period_month + equipment_sk + fuel_brand_id
```

Проверяются:

1. Расхождение источника и результата по ключу:
   ```sql
   ABS(source_liters - COALESCE(output_liters, 0)) > 0.000001
   ```

2. Распределённые строки без поля:
   ```sql
   allocation_status = 'FIELD_ALLOCATED'
   AND pole_id IS NULL
   ```

3. Нераспределённые строки с полем:
   ```sql
   allocation_status = 'NOT_ALLOCATED_TO_FIELD'
   AND pole_id IS NOT NULL
   ```

4. Общие литры источника и результата.

Запуск QA:

```bash
docker exec -i ldali-postgres-dwh \
  psql -X -v ON_ERROR_STOP=1 -P pager=off \
  -U ldali_admin -d ldali_dwh \
  < files/75_mart_kpi5_fuel_field_allocation_scenario_qa.sql
```

Фактический успешный результат:

```text
 bad_keys | field_without_pole | remainder_with_pole | source_liters | output_liters
----------+--------------------+---------------------+---------------+-------------------------------------
        0 |                  0 |                   0 |   637470.1180 | 637470.1180000000000002323749148627
(1 row)
```

---

## Промежуточная проверка выходных строк

До итоговой QA была выполнена сводная проверка представления:

```text
allocation_status         | rows_count | liters                              | field_without_pole | remainder_with_pole
--------------------------+------------+-------------------------------------+--------------------+---------------------
FIELD_ALLOCATED           |       1568 | 517692.8376999699999999942441698627 |                  0 |                   0
NOT_ALLOCATED_TO_FIELD    |        404 | 119777.2803000300000002381307450000 |                  0 |                   0
```

Итог:

- всего строк: `1972`;
- распределено по полям: `517692.83769997` л;
- не распределено по полям: `119777.28030003` л;
- общий объём: `637470.118` л;
- распределённые строки без `pole_id`: `0`;
- нераспределённые строки с `pole_id`: `0`.

---

## Git-коммиты

### Миграция

Коммит:

```text
fffe597 Add KPI5 fuel field allocation scenario view
```

Содержит:

```text
files/75_mart_kpi5_fuel_field_allocation_scenario.sql
```

Изменение:

```text
1 file changed, 104 insertions(+)
```

### QA

Коммит:

```text
8cacaac Add QA for KPI5 fuel field allocation scenario
```

Содержит:

```text
files/75_mart_kpi5_fuel_field_allocation_scenario_qa.sql
```

Изменение:

```text
1 file changed, 56 insertions(+)
```

---

## Важные правила для продолжения

1. Не применять `files/75_mart_kpi5_fuel_field_allocation_scenario.sql` повторно в текущей БД без необходимости: представление уже создано.
2. При изменении определения представления использовать `CREATE OR REPLACE VIEW` либо отдельную управляемую миграцию, а не повторный `CREATE VIEW`.
3. Не встраивать полный QA с многократными обращениями к тяжёлым view в DDL-миграцию.
4. Запускать отдельный QA-файл после изменения №74, №75 либо upstream-представлений, влияющих на распределение топлива.
5. Не запускать несколько тяжёлых диагностических CTE параллельно.
6. При необходимости диагностики активных запросов использовать `pg_stat_activity`.
7. При зависшем `SELECT` отменять конкретный PID через `pg_cancel_backend(pid)`, а не останавливать контейнер PostgreSQL.
8. Для QA считать допустимым отсутствие выходной строки, если источник по ключу равен нулю.
9. Не считать `NOT_ALLOCATED_TO_FIELD` ошибкой: это осознанно сохранённый неподтверждённый остаток.
10. Для витрин DataLens использовать `allocation_status` как обязательный аналитический разрез, чтобы не смешивать подтверждённо полевое топливо с остатком без поля.

---

## Состояние других файлов

Во время сессии в рабочем дереве уже присутствовали сторонние незакоммиченные изменения и новые файлы. Они намеренно не добавлялись в коммиты №75:

```text
M  files/airflow/dags/dag_extract_polya_istoriya.py
?? files/58_mart_field_agro_input_cost_direct_fixed.sql
?? files/59_fix_equipment_repair_cost.sql
?? files/59_mart_fuel_cost_by_equipment_purchase_price.sql
?? files/60_61_62_agro_catalog_rollout.sql
?? files/60_mart_field_fuel_cost_allocated_exact_brand.sql
?? files/61_mart_field_season_direct_cost_components.sql
?? files/63_mart_field_season_kpi6_direct_cost.sql
?? files/70_mart_fuel_by_cost_object.sql
?? session_2026-09-25_kpi6_direct_cost_scenarios.md
```

Перед будущими коммитами проверять staging area через:

```bash
git diff --cached --stat
```

и добавлять файлы адресно через:

```bash
git add -- path/to/file
```

---

## Рекомендуемая дальнейшая работа

1. Запушить коммиты `fffe597` и `8cacaac` в удалённый репозиторий после проверки статуса ветки.
2. Добавить краткую запись в общий `README.md` или статусный документ о новом KPI5-сценарии.
3. Проверить, требуется ли DataLens-набор данных или витрина на основе:
   ```sql
   mart.v_kpi5_fuel_field_allocation_scenario
   ```
4. В DataLens обязательно вывести:
   - период;
   - сезон;
   - технику;
   - марку топлива;
   - статус распределения;
   - поле;
   - агрооперацию;
   - литры;
   - цену без НДС;
   - оценочную стоимость без НДС;
   - метод цены;
   - возраст цены.
5. Не суммировать только `FIELD_ALLOCATED`, если нужен полный объём списания: полный объём равен сумме обоих статусов.
6. При анализе стоимости на поле использовать только `FIELD_ALLOCATED`.
7. При анализе непокрытого распределением топлива использовать `NOT_ALLOCATED_TO_FIELD`.

