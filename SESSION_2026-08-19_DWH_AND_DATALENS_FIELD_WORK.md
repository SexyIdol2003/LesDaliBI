# Сессия 2026-08-19 — аудит DWH и онлайн-источник DataLens для путевых листов

## Цель сессии

Проверить фактическое наполнение DWH «Лесные Дали», определить, какой следующий DataLens-дашборд можно сделать на реально доступных данных, и подготовить источник, который обновляется напрямую из PostgreSQL DWH без CSV.

## Контекст до сессии

- Первый лист DataLens уже создан: «Полеводство — урожай картофеля». На нём показаны валовой сбор по годам, динамика по месяцам, урожай по полям и по сортам.
- DWH работает локально в Docker; контейнер PostgreSQL DWH: `ldali-postgres-dwh`, сервис Compose: `postgres-dwh`.
- Параметры подключения внутри контейнера: пользователь `ldali_admin`, база `ldali_dwh`.
- Для DataLens предусмотрена роль read-only `datalens_ro`.

## Проверенная инфраструктура

На момент проверки были запущены и healthy:

- `ldali-postgres-dwh` — PostgreSQL DWH, порт хоста 5432.
- `ldali-postgres-airflow` — БД Airflow.
- `ldali-airflow-scheduler`.
- `ldali-airflow-web` — порт хоста 8080.

Подключение к DWH из терминала:

```bash
docker compose exec postgres-dwh psql -U ldali_admin -d ldali_dwh
```

## Фактическое наполнение mart

По `pg_stat_user_tables` выявлены ключевые непустые объекты:

| Объект | Оценка строк | Статус |
|---|---:|---|
| `mart.fact_putevoy_rabota` | 25 279 | Основной кандидат для нового BI-листа |
| `mart.fact_harvest` | 5 313 | Используется для листа урожая картофеля |
| `mart.fact_vypusk_urozhaya` | 2 119 | Есть количество и даты, но требуется доработка связки номенклатуры |
| `mart.fact_spisanie_materialov` | 30 | Недостаточно для полноценной аналитики затрат |
| `meta.load_log` | 61 | Технический журнал загрузок |

Также наполнены справочники: `mart.dim_nomenclature` — 4 767 строк, `mart.dim_date` — 4 018, `mart.dim_warehouse` — 925, `mart.dim_equipment` — 169, `mart.dim_crop` — 83, `mart.dim_field` — 48.

Пустыми остаются: `mart.fact_production_output`, `mart.fact_tmc_usage`, `mart.fact_field_costs`, `mart.fact_storage_balance`, `mart.fact_fuel_usage`, `mart.fact_downtime`, `mart.dim_shift`, `mart.dim_downtime_reason`. Поэтому дашборды по сменному производству, простоям, складу, себестоимости и ГСМ пока не строить как управленческие.

## Аудит доступных данных

### Уборка

- `mart.fact_harvest`: 5 313 строк.
- Привязка к полю заполнена на 100%: 5 313 из 5 313 строк.
- `area_ha` и `yield_t_ha` в факте не заполнены.
- Первый DataLens-лист по урожаю картофеля уже создан и подтверждён.

Вывод: можно анализировать валовой сбор, динамику, поля и сорта; пока нельзя корректно строить урожайность в т/га.

### Поля и площадь

- `mart.dim_field`: 48 полей.
- `raw.r1c_polya`: 48 строк.
- `area_ha` не заполнена ни в одном поле: 0 из 48 строк в raw и mart.
- Следствие: расчёт урожайности, затрат на гектар и других hectare-based метрик пока недостоверен.

### Путевые листы

- `mart.fact_putevoy_rabota`: 25 279 строк.
- Техника заполнена во всех строках.
- `pole_id` не заполнен: используется технический UUID `00000000-0000-0000-0000-000000000000`.
- В общей проверке положительный объём работ был обнаружен для 10 319 строк.
- Пробег и выданное топливо в текущем факте не заполнены; моточасы представлены единичными значениями.
- Для части агроопераций объём работ в гектарах заполняется на 100%; проверен период 2021-07-01 — 2026-07-31.
- Справочник `raw.r1c_tech_operations` существует, но пуст: 0 строк. Поэтому названия агроопераций пока недоступны, остаются только `operation_id` / UUID.

### Техника и исполнители

- `mart.dim_equipment` содержит расшифровку техники: `code_1c`, `name`, `eq_type`.
- В путевых листах `tehnika_id` корректно связывается с `mart.dim_equipment.code_1c`.
- `raw.r1c_employees` содержит сотрудников.
- Важное обнаружение: `fact_putevoy_rabota.voditel_id` связывается не с `raw.r1c_employees._id`, а со `raw.r1c_employees.individual_id`.
- Связка по `individual_id` дала 100% сопоставление для всех 10 319 строк с положительным объёмом работ.

### Выпуск урожая

- `mart.fact_vypusk_urozhaya`: 2 119 строк.
- Количество и дата заполнены, `summa` в текущем состоянии не пригодна для финансового анализа.
- Привязка к полю отсутствует.
- Связка `nomenklatura_id` с `mart.dim_nomenclature.ref_key_1c` требует явного приведения `v.nomenklatura_id::uuid`, но сопоставляется только 83 строки из 2 119, то есть 3,9%.

Вывод: дашборд выпуска по товарам пока не публиковать — большинство количества отображается без расшифрованной номенклатуры.

## Созданный онлайн-источник DataLens

В DWH создана view:

```text
mart.v_putevoy_rabota_datalens
```

Определение сохранено в Git:

```text
files/12_putevoy_rabota_datalens_view.sql
```

View берёт данные из `mart.fact_putevoy_rabota`, присоединяет технику через `mart.dim_equipment` и исполнителя через `raw.r1c_employees.individual_id`. Она фильтрует только строки, где `obem_rabot_ga > 0`.

Поля view:

| Поле | Смысл |
|---|---|
| `work_date` | Дата работы |
| `month` | Месяц работы |
| `document_no` | Номер путевого листа |
| `line_number` | Номер строки документа |
| `equipment_id` | Идентификатор техники 1С |
| `equipment` | Название техники |
| `equipment_type` | Тип техники |
| `employee_id` | Идентификатор исполнителя |
| `employee` | Расшифрованный исполнитель |
| `operation_id` | Идентификатор агрооперации; расшифровка пока отсутствует |
| `work_area_ha` | Зарегистрированный объём работ, га |
| `work_norm_ha` | Норма выработки, га |
| `norm_completion_pct` | Выполнение нормы по строке, % |
| `source_doc_id` | Ссылка на документ-источник |
| `source_updated_at` | Время обновления источника |

Роли предоставлен доступ:

```sql
GRANT SELECT ON mart.v_putevoy_rabota_datalens TO datalens_ro;
```

Проверка созданной view:

| Показатель | Значение |
|---|---:|
| Строки | 10 319 |
| Период | 2021-07-01 — 2026-07-31 |
| Зарегистрированный объём работ | 90 954,24 га |
| Единицы техники | 21 |
| Исполнители | 21 |
| Коды операций | 44 |

## Как подключать DataLens

DataLens должен подключаться напрямую к PostgreSQL DWH:

```text
1С → Airflow/OData → raw → mart.fact_putevoy_rabota → mart.v_putevoy_rabota_datalens → DataLens
```

Никакие CSV не являются частью финального решения. CSV `field_work_bi.csv` и `field_work_dataset.csv` создавались только для диагностики и их не нужно добавлять в Git.

Создать dataset:

```text
Подключение: PostgreSQL ldali_dwh под ролью datalens_ro
Схема: mart
Источник: v_putevoy_rabota_datalens
Название dataset: Путевые листы — техника и выработка
```

Рекомендуемый лист:

```text
Полевые работы — техника и выработка
```

Рекомендуемые фильтры: период, тип техники, техника, исполнитель, код операции.

Рекомендуемые KPI:

- `SUM(work_area_ha)` — объём работ, га.
- `COUNTD(document_no)` — путевые листы.
- `COUNTD(equipment)` — активная техника.
- `COUNTD(employee)` — исполнители.
- `SUM(work_area_ha) / SUM(work_norm_ha)` — выполнение нормы; в DataLens оформить как процент.

Рекомендуемые графики:

- Динамика работ по месяцам.
- Top-15 техники по объёму работ.
- Top-15 исполнителей по объёму работ.
- Объём работ по типам техники.
- Тепловая карта: `operation_id × month`, цвет — объём работ.
- Детальная таблица: дата, документ, техника, исполнитель, операция, гектары, норма, выполнение нормы.

Ограничение, которое нужно указать на листе:

> Данные основаны на путевых листах 1С. Показатель отражает зарегистрированный объём работ в га; одна и та же площадь может учитываться в разных агрооперациях. Расшифровка операций, привязка к полям, ГСМ, пробег и моточасы находятся в доработке.

## Git-фиксация

- View создана в БД и предоставлена роли `datalens_ro`.
- SQL definition опубликован в `files/12_putevoy_rabota_datalens_view.sql`.
- Коммит с SQL view: `bb9d693693a690482fb869352d2968f1fe6b937f` (`feat: add DataLens view for field work dashboard`).
- Перед push локальная ветка была отребейжена на удалённый `main`; push завершён успешно.
- Локальные незакоммиченные артефакты, не предназначенные для Git: `field_work_bi.csv`, `field_work_dataset.csv`, `all_entities.txt`, `metadata.xml`, `metadata_utf8.xml`; также нужно игнорировать `airflow/dags/__pycache__/`.

## Приоритет дальнейших работ

1. Создать в DataLens dataset на `mart.v_putevoy_rabota_datalens` и собрать второй лист «Полевые работы — техника и выработка».
2. Починить/дозагрузить `raw.r1c_tech_operations`, чтобы заменить UUID кодов операций на их названия.
3. Найти и загрузить площадь полей: `raw.r1c_polya.area_ha` пусто во всех 48 записях.
4. Восстановить привязку `pole_id` в путевых листах.
5. Дозагрузить ГСМ, пробег и моточасы для последующего дашборда эффективности техники.
6. Разобрать связку выпуска с номенклатурой: текущая сопоставимость с `dim_nomenclature` — 3,9%.
7. Наполнить производственные витрины (`fact_production_output`, `fact_downtime`, `fact_storage_balance`) и только затем делать производственный дашборд.
8. Добавить локальные диагностические артефакты в `.gitignore`, не коммитить CSV и metadata-файлы.

## Повторное подключение к DWH

```bash
cd '/Users/sexyidol2003/Desktop/ЛД БД/LesDaliBI/LesDaliBI/files'
docker compose exec postgres-dwh psql -U ldali_admin -d ldali_dwh
```

## Полезные команды проверки

Проверка новой view:

```bash
docker compose exec postgres-dwh psql -U ldali_admin -d ldali_dwh -c "
SELECT
    COUNT(*) AS rows_cnt,
    MIN(work_date) AS min_date,
    MAX(work_date) AS max_date,
    ROUND(SUM(work_area_ha), 2) AS total_work_area_ha,
    COUNT(DISTINCT equipment) AS equipment_cnt,
    COUNT(DISTINCT employee) AS employee_cnt,
    COUNT(DISTINCT operation_id) AS operation_cnt
FROM mart.v_putevoy_rabota_datalens;
"
```

Проверка статуса Git:

```bash
git status --short
git log --oneline -5
```
