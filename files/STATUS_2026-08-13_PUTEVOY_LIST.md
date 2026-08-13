# LesDaliBI — текущий технический контекст

**Дата:** 13 августа 2026

## Статус

Выгрузка путевых листов тракториста из 1С OData в DWH настроена и проверена полным историческим прогоном. DAG `dag_extract_putevoy_list` успешно завершает все задачи; данные находятся в raw-слое Postgres.

## Проверенный результат

Исторический тест запускался с временной Airflow Variable:

```text
putevoy_lookback_hours = 50000
```

Результат после тестовой загрузки:

| Таблица | Строк |
|---|---:|
| `raw.r1c_putevoy_list` | 12 239 |
| `raw.r1c_putevoy_list_lines` | 25 285 |

После теста значение Variable возвращено в штатное:

```text
putevoy_lookback_hours = 48
```

Штатный ручной прогон с 48-часовым окном завершился `success`. Запланированный прогон также завершился `success`.

## DAG

- **DAG ID:** `dag_extract_putevoy_list`
- **Файл:** `files/airflow/dags/dag_extract_putevoy_list.py`
- **Расписание:** `0 2 * * *` (ежедневно в 02:00)
- **Источник:** 1С OData, документ `Document_АпкПутевойЛистТракториста`
- **Целевые raw-таблицы:**
  - `raw.r1c_putevoy_list`
  - `raw.r1c_putevoy_list_lines`

## Что исправлено

### 1. Поле механизатора

В OData поле называется `Механизатор`, а не `Механизатор_Key`.

- Используется в `$select`: `...,Техника_Key,Механизатор,МоточасовВсего,...`
- В raw-таблицу записывается в `voditel_id`.

### 2. Табличная часть выполненных работ

1С OData не поддерживает получение табличной части `ВыполненныеРаботы` через `$expand`.

Используются два отдельных endpoint:

```text
Document_АпкПутевойЛистТракториста
Document_АпкПутевойЛистТракториста_ВыполненныеРаботы
```

Маппинг строк работ:

| Поле 1С OData | Поле raw |
|---|---|
| `Ref_Key` | `doc_id` |
| `LineNumber` | `line_number` |
| `ВидРаботы_Key` | `agr_operaciya_id` |
| `ЕдиницаДопОбъема_Key` | `edinica_id` |
| `Гектаров` | `obem_rabot_ga` |
| `СменнаяНормаВыработки` | `norma_vyrabotki` |

`pole_id` остаётся `NULL`: в используемой табличной части подходящего поля связи с полем не найдено.

### 3. Очерёдность загрузки

Причина `ForeignKeyViolation` была в параллельном запуске загрузок шапок и строк:

```python
t_extract >> [t_load_docs, t_load_lines] >> t_qc
```

Исправлено на строгую последовательность:

```python
t_extract >> t_load_docs >> t_load_lines >> t_qc
```

Это обязательно: `raw.r1c_putevoy_list_lines.doc_id` имеет внешний ключ на `raw.r1c_putevoy_list._id`, поэтому шапки должны быть зафиксированы до вставки строк.

## Конфигурация Airflow

DAG использует Airflow Variables:

```text
odata_1c_base_url
odata_1c_username
odata_1c_password
odata_1c_page_size          # default: 500
putevoy_lookback_hours      # штатно: 48
odata_1c_timeout_sec        # default: 120
```

Не хранить OData-учётные данные в Git или в Markdown-документации.

## Локальная структура

Рабочий DAG находится по пути:

```text
files/airflow/dags/dag_extract_putevoy_list.py
```

На скриншоте видны потенциально устаревшие/дублирующиеся артефакты, которые нужно проверить перед удалением:

```text
ldali_dwh/
ldali_dwh 2/
ldali_dwh.zip
files/dags/
files/airflow/dags/
```

Не удалять их автоматически без сравнения содержимого и проверки, какой путь используется в `docker-compose.yml` как volume для Airflow DAGs.

## Рекомендации по репозиторию

- Не коммитить `.DS_Store`.
- Добавить в `.gitignore` как минимум:

```gitignore
.DS_Store
__pycache__/
*.pyc
.env
.env.*
airflow/logs/
```

- Не коммитить runtime-логи Airflow, пароли, токены, дампы БД и локальные архивы, если они не являются осознанным релизным артефактом.

## Быстрые команды проверки

Проверить последний run DAG:

```bash
docker exec -i ldali-airflow-scheduler airflow dags list-runs -d dag_extract_putevoy_list
```

Проверить статусы задач конкретного run:

```bash
docker exec -i ldali-airflow-scheduler airflow tasks states-for-dag-run dag_extract_putevoy_list <RUN_ID>
```

Проверить заполнение raw-таблиц:

```bash
docker exec -i ldali-postgres-dwh psql -U ldali_admin -d ldali_dwh -c "SELECT count(*) FROM raw.r1c_putevoy_list; SELECT count(*) FROM raw.r1c_putevoy_list_lines;"
```
