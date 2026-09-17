# Сессия DWH: склады 1С OData и расшифровка объектов затрат

> Дата сессии: 17 сентября 2026
> Репозиторий: `SexyIdol2003/LesDaliBI`
> Цель: загрузить справочник складов из 1С ERP через OData в DWH и найти корректную связь между объектами затрат АПК в документах списания и измерением полей.

## Краткий итог

1. Выгрузка `Catalog_Склады` из 1С OData технически работает. Первый файл содержал 934 строки, но после дедупликации по `Ref_Key` осталось 302 уникальные записи.
2. В `staging.stg_warehouses` успешно загружено 302 записи: 15 групп, 287 складов, 6 удалённых.
3. Группа складов `Склады полей` существует, но среди загруженных записей нет дочерних элементов. Поэтому склады нельзя использовать для расшифровки `apk_cost_object_key`.
4. Витрина `mart.v_agro_input_cost_per_ha_2026` содержит 11 уникальных `apk_cost_object_key` за 2026 год.
5. `apk_cost_object_key` происходит из `raw.r1c_field_material_writeoff_doc.apk_obekt_zatrat_id` и не совпадает с `mart.dim_field.field_code_1c`.
6. Таблицы `raw.r1c_fields` и `raw.r1c_field_material_fact_reg` существуют, но на момент сессии пусты.
7. Создана пустая таблица `staging.map_apk_cost_object_to_field` для подтверждённых соответствий «объект затрат АПК → поле BI». Она пока не заполнена.
8. Запрос к `standard.odata/$metadata` возвращает HTTP 500 со стороны 1С. Автоматически получить список OData EntitySet не удалось.

## Среда

- Локальный проект: `LesDaliBI`.
- Контейнер PostgreSQL: `ldali-postgres-dwh`.
- База: `ldali_dwh`.
- Пользователь PostgreSQL: `ldali_admin`.
- OData endpoint: `http://10.50.254.22/ld_erp_ibiOdata/odata/standard.odata`.
- Рабочая сущность OData: `Catalog_Склады`.
- Учётные данные OData должны храниться только в переменных окружения или локальном `.env`, исключённом из Git.

## Загрузка складов

### Исходная выгрузка

Использовался экспортёр `export_warehouses.py` с entity:

```python
entity = "Catalog_Склады"
```

Выгрузка сообщила:

```text
skip=900 | получено=34 | всего=934
Готово. Складов и групп складов: 934
```

После преобразования JSONL в CSV были сформированы поля:

```text
Ref_Key, DeletionMark, Parent_Key, IsFolder, Description, Подразделение_Key
```

### Причина различия 934 и 302

При вставке в staging по `warehouse_id` возникло нарушение `PRIMARY KEY`: GUID группы `Склады полей` повторялся. После дедупликации `warehouses.csv` по `Ref_Key` осталось 302 уникальные записи.

Вероятная причина: постраничный обход OData использовал `$skip`, но не использовал фиксированный `$orderby`. Для повторной полной выгрузки рекомендуется добавить:

```python
params_base = {
    "$format": "json",
    "$select": "Ref_Key,DeletionMark,Parent_Key,IsFolder,Description,Подразделение_Key",
    "$orderby": "Ref_Key",
}
```

После выгрузки необходимо контролировать равенство числа строк и числа уникальных `Ref_Key`.

### Рабочая таблица staging

```sql
DROP TABLE IF EXISTS staging.stg_warehouses;

CREATE TABLE staging.stg_warehouses (
    warehouse_id text PRIMARY KEY,
    parent_warehouse_id text,
    warehouse_name text,
    is_folder boolean,
    is_deleted boolean,
    department_id text,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

COPY staging.stg_warehouses (
    warehouse_id,
    is_deleted,
    parent_warehouse_id,
    is_folder,
    warehouse_name,
    department_id
)
FROM '/tmp/warehouses_dedup.csv'
WITH (FORMAT csv, HEADER true);
```

Результат загрузки:

```text
records=302
groups=15
warehouses=287
deleted=6
```

### Ветка «Склады полей»

Обнаружена группа:

```text
warehouse_id:        7f101e06-49ec-11f1-b821-005056bde474
parent_warehouse_id: c965aff3-70ff-11eb-9c2e-0050560417d5
warehouse_name:      Склады полей
is_folder:           true
is_deleted:          false
```

Была создана view `mart.v_field_warehouse_map`, рекурсивно ищущая потомков этой группы. Она возвращает 0 строк.

Вывод: `apk_cost_object_key` нельзя маппить на поля через `Catalog_Склады` на текущей выгрузке. Не использовать `mart.v_field_warehouse_map` как источник названия поля в витринах затрат.

## Витрина затрат

Определение `mart.v_agro_input_cost_per_ha_2026` является фильтром `mart.v_agro_input_usage_cost_2026` по категориям `СЗР` и `Удобрение`. Оно содержит `apk_cost_object_key`.

Источник ключа в первичных данных:

```text
raw.r1c_field_material_writeoff_doc.apk_obekt_zatrat_id
→ raw.r1c_agro_writeoff_lines_2026.apk_cost_object_key
→ mart.v_agro_input_usage_cost_2026.apk_cost_object_key
→ mart.v_agro_input_cost_per_ha_2026.apk_cost_object_key
```

`staging.v_field_agro_input_writeoff` также использует этот ключ как `field_id`; по 11 ключам было найдено 444 строк списаний.

## Объекты затрат 2026

| apk_cost_object_key | Площадь, га | Актов | Строк | Материальные затраты без НДС | Строк без цены |
|---|---:|---:|---:|---:|---:|
| f7d1277c-6317-11f1-b821-005056bde474 | 148.000 | 14 | 42 | 14541355.59 | 0 |
| dc26d3fb-60e1-11f1-b821-005056bde474 | 128.000 | 14 | 52 | 11458959.57 | 3 |
| f1c52c28-6317-11f1-b821-005056bde474 | 81.000 | 14 | 42 | 7849244.76 | 0 |
| f7d126a5-6317-11f1-b821-005056bde474 | 68.000 | 13 | 41 | 6475705.69 | 0 |
| bd0ae9e2-5902-11f1-b821-005056bde474 | 57.000 | 14 | 43 | 4311932.21 | 0 |
| f1c52bcf-6317-11f1-b821-005056bde474 | 42.000 | 15 | 44 | 3990914.60 | 0 |
| f1c52b76-6317-11f1-b821-005056bde474 | 26.000 | 14 | 43 | 2319587.81 | 0 |
| 33d2f60c-6309-11f1-b821-005056bde474 | 26.000 | 12 | 29 | 1423648.83 | 12 |
| bd0aea4d-5902-11f1-b821-005056bde474 | 10.000 | 14 | 43 | 765100.39 | 0 |
| c311f13d-5902-11f1-b821-005056bde474 | 9.000 | 14 | 43 | 718619.22 | 0 |
| f7d1264c-6317-11f1-b821-005056bde474 | 1.200 | 13 | 41 | 112490.86 | 0 |

Нельзя сопоставлять эти объекты с полями только по площади: площадь не является стабильным уникальным ключом.

## Справочник полей

`mart.dim_field` заполнена 48 действующими полями со структурой:

```text
field_sk bigint
field_code_1c text
field_name text
department text
area_ha numeric
organization text
is_active boolean
valid_from date
valid_to date
is_current boolean
```

Например:

| field_sk | field_code_1c | field_name |
|---:|---|---|
| 30 | d87d0832-f2f7-11eb-9c3a-00505689ff45 | 201 |
| 31 | fbf68bb5-f2f5-11eb-9c3a-00505689ff45 | 203 |
| 39 | d7ca999f-f153-11eb-9c3a-00505689ff45 | 318 |
| 8 | 3ad6a04c-f2f1-11eb-9c3a-00505689ff45 | 751 |
| 17 | 508b9451-f156-11eb-9c3a-00505689ff45 | 992 |

Ни один приведённый `field_code_1c` не совпадает с ключами `apk_cost_object_key`, поэтому прямой JOIN по ним сейчас некорректен.

На момент сессии источники не заполнены:

```text
raw.r1c_fields = 0 строк
raw.r1c_field_material_fact_reg = 0 строк
```

Структура `raw.r1c_fields`:

```text
_id text
_deletionmark boolean
description text
parent_id text
area_ha numeric
farm_id text
_loaded_at timestamptz
```

Структура `raw.r1c_field_material_fact_reg`:

```text
period date
organizaciya_id uuid
vid_raboty_id uuid
nomenklatura_id uuid
obekt_zatrat_id uuid
sklad_id uuid
raskhod_na_ga numeric
kolichestvo numeric
ploshchad_obrabotannaya numeric
recorder_doc_id uuid
_loaded_at timestamptz
```

Этот регистр сейчас пуст и не содержит отдельного поля `field_id`, поэтому сам по себе также не гарантирует связь объекта затрат с `mart.dim_field`.

## Созданная таблица соответствий

Во время сессии создана таблица:

```sql
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
```

Текущее состояние: **0 строк**.

Необходимо заполнять только подтверждёнными парами:

```text
apk_cost_object_key → mart.dim_field.field_sk
```

Не выполнять тестовые `INSERT` с предположительными связями.

## OData metadata

Проверка:

```text
GET http://10.50.254.22/ld_erp_ibiOdata/odata/standard.odata/$metadata
```

вернула:

```text
HTTP 500
Произошла внутренняя ошибка OData сервиса.
Дополнительные сведения можно найти в технологическом журнале.
```

При этом `Catalog_Склады` успешно выгружается, а заведомо отсутствующая сущность возвращает штатный HTTP 404.

Следовательно, доступ к endpoint и авторизация работают, но сервер 1С не формирует OData metadata.

## Что запросить у администратора 1С

Передать следующий запрос:

> При вызове `http://10.50.254.22/ld_erp_ibiOdata/odata/standard.odata/$metadata` OData возвращает HTTP 500: «Произошла внутренняя ошибка OData сервиса. Дополнительные сведения можно найти в технологическом журнале». Просьба проверить технологический журнал и публикацию OData `standard.odata`. Также нужен точный OData EntitySet справочника, на который ссылается реквизит `АпкОбъектЗатрат` (`apk_obekt_zatrat_id`) документа списания материалов на поля. Для DWH нужны как минимум GUID и наименование записи.

Нужен один из результатов:

1. Рабочий `$metadata`; или
2. Точное имя EntitySet для справочника объектов затрат АПК; или
3. Выгрузка из 1С с колонками `Ref_Key`, `Description` и, если возможно, с явной ссылкой на поле.

## План следующей сессии

1. Не изменять и не заполнять `staging.map_apk_cost_object_to_field` до получения подтверждённых бизнес-соответствий.
2. Получить от 1С точное имя OData EntitySet для `АпкОбъектЗатрат` либо исправить `$metadata`.
3. Сделать тест `?$format=json&$top=3` для подтверждённого EntitySet.
4. Экспортировать справочник с фиксированным `$orderby=Ref_Key`.
5. Дедуплицировать выгрузку по `Ref_Key` и загрузить raw/staging-слой.
6. Если объект затрат содержит прямую ссылку на поле, автоматически заполнить `staging.map_apk_cost_object_to_field`.
7. Если прямой ссылки нет, запросить/построить подтверждённую карту соответствий в 1С.
8. Только затем создать отдельную reporting view, которая добавит в `mart.v_agro_input_cost_per_ha_2026` поля `field_sk` и `field_name`. Не менять исходную витрину без проверки.
9. После заполнения карты добавить контроль качества: число unmapped объектов, сумма unmapped затрат, число unmapped строк.

## Команды безопасности

### Проверить текущую карту

```sql
SELECT *
FROM staging.map_apk_cost_object_to_field
ORDER BY apk_cost_object_key;
```

### Не показывать и не коммитить пароль

Использовать только переменные окружения текущей shell-сессии:

```bash
export ODATA_BI_USER='OdataBi'
read -s 'ODATA_BI_PASSWORD?Введите пароль OData: '
echo
export ODATA_BI_PASSWORD
```

Перед коммитом убедиться, что `.env`, файлы выгрузок и CSV/JSONL не попадают в индекс Git.

### Типичная ошибка терминала

SQL-команды нужно всегда передавать через `psql`:

```bash
docker exec -i ldali-postgres-dwh psql -U ldali_admin -d ldali_dwh <<'SQL'
SELECT now();
SQL
```

Нельзя вставлять `SELECT`, `INSERT`, GUID или результаты psql напрямую в приглашение `zsh`; это приводит к `command not found` или `parse error`, но не меняет PostgreSQL.
