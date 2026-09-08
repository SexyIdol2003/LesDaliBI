# Состояние BI/DWH после сессии 2026-09-08

> Дата фиксации: 8 сентября 2026
> Репозиторий: `SexyIdol2003/LesDaliBI`
> Контур: 1С:ERP Агропромышленный комплекс -> OData -> Airflow -> PostgreSQL DWH -> DataLens

## Краткий итог

В сессии подтверждена работоспособность контура загрузки и витрины фактического расхода ГСМ. В DWH доступен факт расхода топлива по технике и месяцам. Подготовлены и развернуты DAG-и для загрузки сводного списания топлива, ремонтов техники и списаний материалов на поля.

Единственный внешний блокер: технический пользователь `OdataBi` получает `HTTP 403 Forbidden` при чтении OData-сущности `Document_ДвижениеПродукцииИМатериалов`. Поэтому фактические списания удобрений и СЗР на поля пока не могут быть загружены.

## Что подтверждено

### Платформа

- Airflow успешно читает DAG-и из `/opt/airflow/dags`; синтаксическая проверка `dag_extract_agro_input_writeoff.py` завершалась с кодом `0`.
- Планировщик Airflow видит 13 DAG-ов, включая новые `dag_extract_fuel_summary`, `dag_extract_equipment_repair` и `dag_extract_agro_input_writeoff`.
- PostgreSQL DWH доступен в контейнере `ldali-postgres-dwh`, база `ldali_dwh`.
- Публикация 1С OData доступна по адресу `http://10.50.254.22/ld_erp_ibiOdata/odata/standard.odata`.

### Факт ГСМ

Витрина `mart.fact_fuel_writeoff` заполнена и связана с `mart.dim_equipment`. По результатам проверки в ней есть: 

- 61 проведённый документ списания топлива;
- 561 строка фактического расхода;
- 22 единицы техники;
- период данных: июль 2021 — июль 2026;
- суммарный расход: 619 375,0 л;
- строк без сопоставления с `mart.dim_equipment`: 0.

Пример результата за июль 2026:

| Техника | Расход, л |
|---|---:|
| Трактор К-701, гос. № 1919ХР50 | 3 123,0 |
| Lovol 2604, гос. № 6492ХХ50 | 2 374,9 |
| Трактор К-742М, гос. № 1880ХР50 | 1 942,7 |
| Lovol 2604, гос. № 9131ХХ50 | 1 655,2 |
| John Deere, гос. № 6494ХХ50 | 1 027,3 |

Полезный запрос для DataLens / проверки витрины:

```sql
SELECT
  f.period_month AS month,
  e.name AS equipment,
  ROUND(SUM(f.liters_consumed), 1) AS fuel_consumed_liters,
  ROUND(SUM(f.liters_refueled), 1) AS fuel_refueled_liters,
  ROUND(SUM(f.liters_start), 1) AS fuel_start_balance_liters,
  ROUND(SUM(f.liters_end), 1) AS fuel_end_balance_liters
FROM mart.fact_fuel_writeoff f
JOIN mart.dim_equipment e
  ON e.eq_sk = f.equipment_sk
GROUP BY f.period_month, e.name
ORDER BY f.period_month DESC, fuel_consumed_liters DESC;
```

## Ремонты техники

Подготовлены физическая модель и DAG `dag_extract_equipment_repair`. OData-источник отвечает, однако полезный факт для BI пока отсутствует:

- найдено 3 документа ремонта;
- 1 документ проведён и не помечен на удаление, но не содержит строк работ;
- 2 документа содержат 240 часов и 24 000 ₽, но являются удалёнными/непроведёнными и не должны попасть в BI-факт;
- вследствие этого mart-витрина ремонтов пока содержит 0 валидных строк.

Это не ошибка пайплайна: в доступном источнике нет проведённого факта ремонтных работ.

## Списания материалов на поля

Созданы SQL-структура `files/24_raw_field_material_writeoff.sql` и DAG `dag_extract_agro_input_writeoff.py`, который должен получать документы движения продукции и материалов, отбирать требуемый вид документа и формировать факт списания материалов на поле.

Контрольный тест Airflow:

```bash
airflow tasks test dag_extract_agro_input_writeoff verify_entity 2026-09-08
```

не проходит на строке `response.raise_for_status()` с ошибкой:

```text
requests.exceptions.HTTPError: 403 Client Error: Forbidden
```

Проверяется минимальный запрос без серверного фильтра:

```text
Document_ДвижениеПродукцииИМатериалов?$format=json&$select=Ref_Key,АпкВидДокумента&$top=50
```

Следовательно, проблема не в OData-фильтре, URL-кодировании, Airflow или SQL, а в правах чтения сущности либо в ограничении состава опубликованного OData-сервиса. Повторные тесты до изменения прав не нужны.

### Запрос 1С-администратору

Для технического пользователя `OdataBi` требуется предоставить только чтение через публикацию `ld_erp_ibiOdata/odata/standard.odata` к:

- `Документ.ДвижениеПродукцииИМатериалов`;
- табличной части `Товары`;
- связанным справочникам, необходимым для аналитики: номенклатура, поля/объекты затрат, виды работ, склады и группы продукции.

Права на создание, изменение, проведение и удаление документов для BI не требуются.

## Что сделано на Mac / в контейнерах

В локальном контуре были подготовлены и применены изменения:

- скорректированы SQL-файлы `22_raw_fuel_writeoff_summary.sql`, `23_raw_equipment_repair.sql`, `24_raw_field_material_writeoff.sql` с учетом реальных ключей DWH: `eq_sk`, `code_1c`, `field_code_1c`;
- обновлён `dag_extract_fuel_summary.py`;
- добавлены `dag_extract_equipment_repair.py` и `dag_extract_agro_input_writeoff.py`;
- выполнена проверка `python3 -m py_compile` для DAG списаний материалов;
- файлы DAG были переданы в контейнеры `ldali-airflow-scheduler` и `ldali-airflow-web` командой `docker cp`;
- контейнеры `airflow-scheduler` и `airflow-webserver` перезапускались;
- выполнена сериализация DAG-ов командой `airflow dags reserialize`.

## Важный риск сохранности

Текущие DAG-и попали в исполняемые контейнеры через `docker cp`, а не через bind mount / Docker volume. Это означает, что после `docker compose down`, пересоздания контейнеров или развертывания на другом хосте локальные правки могут исчезнуть из `/opt/airflow/dags`, если исходники не зафиксированы в Git и не подключены через `docker-compose.yml`.

До следующего перезапуска необходимо:

1. Сверить локальные версии SQL и DAG-ов с репозиторием.
2. Закоммитить фактические исходники из Mac-рабочей директории.
3. Настроить в `files/docker-compose.yml` постоянное монтирование каталога DAG-ов, например локального `./airflow/dags` в `/opt/airflow/dags` для scheduler и webserver.
4. Не помещать в Git `.env`, пароли, Basic Auth-заголовки, cookies, токены, экспортные JSON-ответы с чувствительными данными и временные файлы.

## Что пока не зафиксировано удалённо

По доступу GitHub нельзя прочитать автоматически содержимое файлов, которые изменялись только на Mac или были скопированы в контейнеры. В ветке `main` на момент проверки отсутствуют:

- `files/airflow/dags/dag_extract_equipment_repair.py`;
- `files/airflow/dags/dag_extract_agro_input_writeoff.py`.

Кроме того, нельзя утверждать, что версии `files/22_raw_fuel_writeoff_summary.sql`, `files/23_raw_equipment_repair.sql`, `files/24_raw_field_material_writeoff.sql` и `files/airflow/dags/dag_extract_fuel_summary.py` в Git идентичны рабочим версиям с Mac: для этого нужно передать их фактическое содержимое или выполнить локальную сверку `git diff`.

## Следующий порядок действий

1. Получить подтверждение 1С-администратора об открытии чтения `Document_ДвижениеПродукцииИМатериалов`.
2. Выполнить один тест `verify_entity`; при HTTP 200 запустить DAG загрузки списаний материалов.
3. Проверить строки в raw-слое и mart-витрине, включая поле, культуру, номенклатуру, количество, единицу измерения, сумму и дату документа.
4. Зафиксировать с Mac фактические SQL/DAG-файлы и обновить `docker-compose.yml` для постоянного mount DAG-ов.
5. Подключить `mart.fact_fuel_writeoff` в DataLens и собрать первый график расхода ГСМ по месяцу и технике.
