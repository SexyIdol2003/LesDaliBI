# Лесные дали — локальный DWH-стек

Этап 1.1 плана-графика. Локальное развёртывание PostgreSQL + Airflow + dbt + pgAdmin
для разработки витрин полеводства и производства.

## Что внутри

- **postgres-dwh** (порт 5432) — основной DWH с разделёнными слоями `raw`/`staging`/`mart`/`meta`.
  Схемы и звёздная схема витрин создаются автоматически при первом старте контейнера.
- **postgres-airflow** — отдельная БД для метаданных Airflow.
- **airflow** (порт 8080) — оркестратор: scheduler + webserver, LocalExecutor.
  Логин/пароль: `admin` / `admin`.
- **pgadmin** (порт 5050) — GUI для PostgreSQL, оба сервера уже подключены.
  Логин/пароль: `admin@ldali.local` / `admin`.
- **dbt** — контейнер для ручных запусков (`docker compose run --rm dbt ...`).

## Запуск

```bash
# первый старт — соберёт образы, инициализирует Airflow, создаст схемы DWH
docker compose up -d

# проверить, что всё поднялось
docker compose ps

# логи (если что-то пошло не так)
docker compose logs -f postgres-dwh
docker compose logs -f airflow-scheduler
```

После старта:
- pgAdmin: <http://localhost:5050> — слева увидишь два сервера; пароль к DWH:
  `ldali_admin_change_me` (для DWH), `airflow` (для метаданных Airflow)
- Airflow: <http://localhost:8080> — есть DAG `smoke_test_dwh`, запусти его руками
  для проверки, что Airflow видит DWH

## Подключение DataLens / внешних инструментов

Для аналитики используй пользователя `datalens_ro` (read-only на схему `mart`):

```
host: localhost
port: 5432
db:   ldali_dwh
user: datalens_ro
pass: datalens_ro_change_me
```

## Слои хранилища

```
raw     -- сырые данные из 1С OData / Google Sheets / Яндекс.Диск, без обработки
staging -- типизированные, очищенные, с разобранными артикулами 1С
mart    -- звёздная схема: dim_date, dim_field, dim_crop, dim_nomenclature,
           dim_equipment, dim_warehouse, dim_shift, dim_downtime_reason
           + fact_harvest, fact_field_costs, fact_tmc_usage, fact_fuel_usage,
             fact_production_output, fact_storage_balance, fact_downtime
meta    -- журнал загрузок, словари маппингов
```

## Что дальше

1. Подключить выгрузку 1С OData → `raw.r1c_*` (Airflow DAG `etl_1c_daily`).
2. Подключить Google Sheets производства → `raw.gs_production_shift` (DAG `etl_gsheets_daily`).
3. dbt-модели `staging.stg_*` — типизация, парсинг артикулов в crop+nom+caliber+pack.
4. dbt-модели `marts.dim_*`, `marts.fact_*` — наполнение витрин из staging.
5. После 8 недели — подключить Yandex DataLens к схеме `mart` (пользователь
   `datalens_ro`) и собрать дашборд «Полеводство».

## Безопасность

Пароли в этом стеке — для локальной разработки. Перед выкаткой на сервер компании
поменяй все `*_change_me` и `admin/admin`, а также вынеси секреты в `.env`.

## Перезапуск с нуля

```bash
docker compose down -v       # снесёт и тома (всю БД)
docker compose up -d
```
