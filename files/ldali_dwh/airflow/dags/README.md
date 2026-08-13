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
01:00  dag_extract_catalogs        (справочники)
02:00  dag_extract_putevoy_list    (путевые листы)
02:00, 04:00, ... dag_extract_dvizhenie (каждые 2 часа)
03:30  dag_transform_mart          (трансформация в mart)
```

## Airflow Variables (обязательные)

Задать через Admin → Variables в Airflow UI или CLI:

| Variable | Пример значения | Описание |
|----------|-----------------|----------|
| `odata_1c_base_url` | `http://1c-server/lesdali/odata/standard.odata` | Базовый URL OData-сервиса |
| `odata_1c_username` | `bi_reader` | Логин для Basic Auth |
| `odata_1c_password` | `********` | Пароль |
| `odata_1c_page_size` | `500` | Размер страницы (`$top`) |
| `odata_1c_timeout_sec` | `120` | Таймаут HTTP-запроса, сек |
| `dvizhenie_lookback_hours` | `48` | Глубина инкремента для документа Движение |
| `putevoy_lookback_hours` | `48` | Глубина инкремента для путевых листов |

## Airflow Connections (обязательные)

| Connection ID | Тип | Описание |
|---------------|-----|----------|
| `ldali_postgres` | Postgres | Подключение к DWH (raw + mart схемы) |

## Порядок разворачивания

1. Проверить доступность OData: `curl -u user:pass http://<server>/<base>/odata/standard.odata/`
2. Положить все `.py` файлы DAG'ов в `files/dags/` (или в `dags/` папку Airflow согласно `docker-compose.yml`)
3. Задать Variables и Connection в Airflow UI
4. Запустить `dag_extract_catalogs` вручную первый раз (полная инициализация справочников)
5. Включить остальные DAG'и по расписанию

## Зависимости (requirements.txt для Airflow)

```
apache-airflow-providers-postgres
requests
```
