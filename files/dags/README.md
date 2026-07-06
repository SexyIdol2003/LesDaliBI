# Airflow DAGs — Лесные Дали BI

## Состав DAG'ов

| DAG | Что делает | Расписание | Источник |
|-----|-----------|------------|----------|
| `dag_extract_catalogs` | Полная выгрузка 3 справочников (Упаковки, СХТехника, Поля) | `0 1 * * *` (01:00 ночью) | OData 1С |
| `dag_extract_dvizhenie` | Инкрементальная выгрузка Document_ДвижениеПродукцииИМатериалов | `0 */2 * * *` (каждые 2 часа) | OData 1С |
| `dag_extract_putevoy_list` | Инкрементальная выгрузка Document_АпкПутевойЛистТракториста | `0 2 * * *` (02:00 ночью) | OData 1С |
| `dag_transform_mart` | RAW → MART: split по Операции + путевые листы | `30 3 * * *` (03:30, после выгрузок) | PostgreSQL raw-слой |

## Порядок выполнения за сутки

```
01:00  dag_extract_catalogs
02:00  dag_extract_putevoy_list
02:00, 04:00, ...  dag_extract_dvizhenie (каждые 2 часа)
03:30  dag_transform_mart
```

## Airflow Variables (обязательные)

| Variable | Пример | Описание |
|----------|--------|----------|
| `odata_1c_base_url` | `http://1c-server/lesdali/odata/standard.odata` | Базовый URL OData |
| `odata_1c_username` | `bi_reader` | Логин Basic Auth |
| `odata_1c_password` | `********` | Пароль |
| `odata_1c_page_size` | `500` | Размер страницы (`$top`) |
| `odata_1c_timeout_sec` | `120` | Таймаут HTTP, сек |
| `dvizhenie_lookback_hours` | `48` | Глубина инкремента для Движения |
| `putevoy_lookback_hours` | `48` | Глубина инкремента для путевых листов |

## Connections

| Connection ID | Тип | Описание |
|---------------|-----|----------|
| `ldali_postgres` | Postgres | DWH (raw + mart схемы) |

## Порядок запуска

1. Применить `05_mart_facts.sql` к БД
2. Задать Variables и Connection в Airflow UI
3. Запустить `dag_extract_catalogs` вручную первый раз
4. Включить остальные DAG'и по расписанию
