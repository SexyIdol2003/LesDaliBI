# Отчёт о верификации проекта LesDali BI — 13.07.2026

---

## 1. Структура проекта и ключевой код

### 1.1 Структура репозитория

```
LesDaliBI/
├── files/
│   ├── 01_schemas.sql          — создание схем raw, staging, mart, meta
│   ├── 02_raw.sql              — DDL таблиц сырого слоя (30+ таблиц из 1С)
│   ├── 03_mart.sql             — DDL витрин: dim_date, dim_nomenklatura и др.
│   ├── 05_mart_facts.sql       — DDL fact-таблиц: fact_vypusk, fact_spisanie, fact_putevoy
│   ├── docker-compose.yml      — инфраструктура: 5 контейнеров
│   └── dags/                   — DAG-файлы Airflow
├── DEMO_SESSION_2026-07-13.md
└── README.md
```

### 1.2 Docker-инфраструктура (docker-compose.yml)

Файл: [`files/docker-compose.yml`](https://github.com/SexyIdol2003/LesDaliBI/blob/main/files/docker-compose.yml)

Запущенные контейнеры:
- `ldali-postgres-dwh` — PostgreSQL DWH
- `ldali-postgres-airflow` — PostgreSQL для метаданных Airflow
- `ldali-airflow-web` — Airflow Webserver
- `ldali-airflow-scheduler` — Airflow Scheduler
- `ldali-pgadmin` — pgAdmin UI

Проверка статуса:
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

### 1.3 Настройка коннектора 1С OData в Airflow

```bash
docker exec -it ldali-airflow-web airflow connections delete odata_1c_ld

docker exec -it ldali-airflow-web airflow connections add odata_1c_ld \
  --conn-type http \
  --conn-host "http://10.50.254.22/ld_erp_ibOdata/odata/standard.odata" \
  --conn-login "OdataBi" \
  --conn-password "xxx123"
```

### 1.4 Переменные Airflow

```bash
docker exec -it ldali-airflow-web airflow variables set odata_1c_username "OdataBi"
docker exec -it ldali-airflow-web airflow variables set odata_1c_password "xxx123"
docker exec -it ldali-airflow-web airflow variables set odata_1c_base_url "http://10.50.254.22/ld_erp_ibOdata/odata/standard.odata"
docker exec -it ldali-airflow-web airflow variables set odata_1c_page_size "500"
```

### 1.5 SQL — создание схем (01_schemas.sql)

Файл: [`files/01_schemas.sql`](https://github.com/SexyIdol2003/LesDaliBI/blob/main/files/01_schemas.sql)

### 1.6 SQL — fact-таблицы витрин (05_mart_facts.sql)

Файл: [`files/05_mart_facts.sql`](https://github.com/SexyIdol2003/LesDaliBI/blob/main/files/05_mart_facts.sql)

Ключевые таблицы с PRIMARY KEY (защита от дублей):
```sql
CREATE TABLE IF NOT EXISTS mart.fact_vypusk_urozhaya (
    doc_id TEXT, doc_number TEXT, doc_date TIMESTAMP,
    pole_id TEXT, nomenklatura_id TEXT, edinica_id TEXT,
    kolichestvo NUMERIC, summa NUMERIC, seriya TEXT,
    _updated_at TIMESTAMP DEFAULT now(),
    CONSTRAINT pk_fact_vypusk PRIMARY KEY (doc_id, pole_id, nomenklatura_id)
);

CREATE TABLE IF NOT EXISTS mart.fact_spisanie_materialov (
    doc_id TEXT, doc_number TEXT, doc_date TIMESTAMP,
    pole_id TEXT, nomenklatura_id TEXT, edinica_id TEXT,
    kolichestvo NUMERIC, summa NUMERIC, seriya TEXT,
    _updated_at TIMESTAMP DEFAULT now(),
    CONSTRAINT pk_fact_spisanie PRIMARY KEY (doc_id, pole_id, nomenklatura_id)
);

CREATE TABLE IF NOT EXISTS mart.fact_putevoy_rabota (
    doc_id TEXT, doc_number TEXT, doc_date TIMESTAMP,
    tehnika_id TEXT, model_tehniki_id TEXT, voditel_id TEXT,
    pole_id TEXT, agr_operaciya_id TEXT, edinica_id TEXT,
    obem_rabot_ga NUMERIC, norma_vyrabotki NUMERIC,
    narabotka_moto_chas NUMERIC, probeg_km NUMERIC,
    toplivo_vydano NUMERIC, toplivo_vozvrat NUMERIC,
    _updated_at TIMESTAMP DEFAULT now(),
    CONSTRAINT pk_fact_putevoy PRIMARY KEY (doc_id, pole_id, agr_operaciya_id)
);
```

---

## 2. Данные в raw-слое (загружены из 1С ERP)

Проверка через терминал:
```bash
docker exec -it ldali-postgres-dwh psql -U ldali_admin -d ldali_dwh -c "
SELECT relname AS table_name, n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname = 'raw' AND n_live_tup > 0
ORDER BY rows DESC;"
```

Результат на 13.07.2026:

| Таблица | Строк |
|---|---|
| r1c_nomenclature | 15 846 |
| catalog_номенклатура | 15 807 |
| r1c_counterparties | 1 444 |
| catalog_контрагенты | 1 443 |
| r1c_warehouses | 925 |
| catalog_склады | 925 |
| r1c_org_units | 33 |
| catalog_подразделенияорганизаций | 33 |
| r1c_currency | 1 |

---

## 3. Airflow — DAG-процессы

### 3.1 Список DAGов

```bash
docker exec -it ldali-airflow-web airflow dags list
```

Активные DAGи:
- `dag_extract_dvizhenie` — выгрузка движений продукции из 1С
- `dag_extract_putevoy_list` — выгрузка путевых листов из 1С
- `dag_transform_mart` — трансформация raw → mart
- *(дополнительные DAGи по каталогам)*

### 3.2 Ручной запуск DAGов

```bash
# Запуск выгрузки из 1С
docker exec -it ldali-airflow-web airflow dags trigger dag_extract_dvizhenie
docker exec -it ldali-airflow-web airflow dags trigger dag_extract_putevoy_list

# После завершения — трансформация в mart
docker exec -it ldali-airflow-web airflow dags trigger dag_transform_mart
```

### 3.3 Проверка статуса последних запусков

```bash
docker exec -it ldali-airflow-web airflow dags list-runs \
  --dag-id dag_extract_dvizhenie --limit 5

docker exec -it ldali-airflow-web airflow dags list-runs \
  --dag-id dag_transform_mart --limit 5
```

---

## 4. Таблицы и аналитические витрины — финальная проверка

```bash
docker exec -it ldali-postgres-dwh psql -U ldali_admin -d ldali_dwh -c "
SELECT schemaname, relname AS tablename, n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname IN ('raw', 'mart') AND n_live_tup > 0
ORDER BY schemaname, rows DESC;"
```

Просмотр содержимого витрины dim_date:
```bash
docker exec -it ldali-postgres-dwh psql -U ldali_admin -d ldali_dwh -c "
SELECT * FROM mart.dim_date LIMIT 10;"
```

Просмотр номенклатуры:
```bash
docker exec -it ldali-postgres-dwh psql -U ldali_admin -d ldali_dwh -c "
SELECT * FROM raw.r1c_nomenclature LIMIT 10;"
```

---

## 5. Проверка идемпотентности (отсутствие дублей)

Шаг 1 — запустить DAG повторно:
```bash
docker exec -it ldali-airflow-web airflow dags trigger dag_extract_dvizhenie
docker exec -it ldali-airflow-web airflow dags trigger dag_transform_mart
```

Шаг 2 — проверить дубли в fact-таблицах:
```bash
docker exec -it ldali-postgres-dwh psql -U ldali_admin -d ldali_dwh -c "
SELECT doc_id, pole_id, nomenklatura_id, COUNT(*)
FROM mart.fact_vypusk_urozhaya
GROUP BY doc_id, pole_id, nomenklatura_id
HAVING COUNT(*) > 1;"
```

Ожидаемый результат: 0 строк (дублей нет, работает upsert через PRIMARY KEY).

---

## 6. Инструкция по скриншотам

Сделать следующие скриншоты:

**Airflow UI (http://localhost:8080)**
- Главная страница DAGs: список всех DAGов, столбцы Last Run, Schedule, Success/Failed
- Страница конкретного DAG (dag_extract_dvizhenie или dag_transform_mart): граф задач с зелёными кружками
- Вкладка Grid или Calendar: история успешных запусков
- Вкладка Logs одного успешного запуска: лог с INFO строками
- Admin → Connections: коннектор `odata_1c_ld` и `postgres_dwh`
- Admin → Variables: переменные `odata_1c_base_url`, `odata_1c_page_size`

**pgAdmin (http://localhost:5050)**
- Дерево объектов: схемы raw, staging, mart, meta с раскрытыми таблицами
- Query Tool: результат SELECT из `raw.r1c_nomenclature` с реальными данными
- Query Tool: результат SELECT из `mart.dim_date` с реальными данными
- Query Tool: результат финальной проверки строк по всем схемам

**Терминал**
- Вывод `docker ps` — все 5 контейнеров Up
- Вывод команды проверки raw-таблиц с количеством строк

---

## 7. Инструкция по записи видео

Записать одно видео продолжительностью 3-5 минут. Использовать OBS или встроенную запись экрана.

Порядок демонстрации:
1. Открыть терминал, показать `docker ps` — все контейнеры Up
2. Открыть Airflow UI (http://localhost:8080), показать список DAGов
3. В терминале выполнить trigger одного DAG:
   ```bash
   docker exec -it ldali-airflow-web airflow dags trigger dag_extract_dvizhenie
   ```
4. Вернуться в Airflow UI, обновить страницу — показать что DAG запустился (Running)
5. Дождаться завершения (или показать уже успешный запуск) — зелёный кружок
6. Открыть pgAdmin, выполнить SELECT из raw-таблицы — показать реальные данные из 1С
7. Выполнить trigger dag_transform_mart, потом показать mart-таблицу

---

## Доступы (для воспроизведения)

- Airflow UI: http://localhost:8080 — admin / admin
- pgAdmin: http://localhost:5050 — admin@admin.com / admin
- PostgreSQL: ldali-postgres-dwh:5432 — ldali_admin / ldali_admin_change_me
- 1С OData: http://10.50.254.22/ld_erp_ibOdata/odata/standard.odata — OdataBi / xxx123
