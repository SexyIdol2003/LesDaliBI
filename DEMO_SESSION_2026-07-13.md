# 🗓 Демо-сессия 13.07.2026 — Прогресс и статус

## ✅ ЧТО СДЕЛАНО СЕГОДНЯ

### 1. Инфраструктура
- Docker-окружение поднято и работает 4+ дней
- Все 5 контейнеров Up: `ldali-postgres-dwh`, `ldali-postgres-airflow`, `ldali-airflow-web`, `ldali-airflow-scheduler`, `ldali-pgadmin`
- pgAdmin пересоздан с новым email `admin@admin.com` / пароль `admin` (старый `admin@ldali.local` не работал)

### 2. Схемы БД
- Все схемы созданы: `raw`, `staging`, `mart`, `meta`
- raw содержит 30+ таблиц из 1С

### 3. Airflow — исправления
- **Проблема 1:** Логин 1С был `Бортков ИИ` (кириллица) → UnicodeEncodeError в latin-1
- **Решение:** Пересоздан коннектор `odata_1c_ld` через CLI:
```bash
docker exec -it ldali-airflow-web airflow connections delete odata_1c_ld
docker exec -it ldali-airflow-web airflow connections add odata_1c_ld \
  --conn-type http \
  --conn-host "http://10.50.254.22/ld_erp_ibOdata/odata/standard.odata" \
  --conn-login "OdataBi" \
  --conn-password "xxx123"
```
- **Проблема 2:** Variables `odata_1c_username` и `odata_1c_password` отсутствовали
- **Решение:** Добавлены через CLI:
```bash
docker exec -it ldali-airflow-web airflow variables set odata_1c_username "OdataBi"
docker exec -it ldali-airflow-web airflow variables set odata_1c_password "xxx123"
docker exec -it ldali-airflow-web airflow variables set odata_1c_base_url "http://10.50.254.22/ld_erp_ibOdata/odata/standard.odata"
docker exec -it ldali-airflow-web airflow variables set odata_1c_page_size "500"
```

### 4. Данные в raw (загружены из 1С)
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

### 5. Mart — исправления
- **Проблема:** `dag_transform_mart` падал — таблицы `fact_vypusk_urozhaya`, `fact_spisanie_materialov`, `fact_putevoy_rabota` не существовали
- **Решение:** Созданы вручную:
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

### 6. Mart — текущие данные
| Таблица | Строк |
|---|---|
| dim_date | 4 018 |
| fact_vypusk_urozhaya | ? (transform запущен) |
| fact_spisanie_materialов | ? (transform запущен) |
| fact_putevoy_rabota | ? (transform запущен) |

---

## 🔴 НА ЧЁМ ОСТАНОВИЛИСЬ

**Статус на 21:44 МСК:**
- `dag_extract_dvizhenie` и `dag_extract_putevoy_list` — **последний запуск 18:07**, неизвестно завершились ли успешно
- `dag_transform_mart` — запущен в 18:38, статус неизвестен
- raw-таблицы `r1c_dvizhenie_produkcii` и `r1c_putevoy_list` — **не подтверждено наличие данных**
- mart fact-таблицы — возможно пустые если dvizhenie/putevoy не отработали

## 🔧 ЧТО ДЕЛАТЬ СЛЕДУЮЩИМ ШАГОМ

### Шаг 1 — Проверить статус DAGов
```bash
# Открыть http://localhost:8080 → посмотреть цвет кружков
```

### Шаг 2 — Проверить данные в raw
```bash
docker exec -it ldali-postgres-dwh psql -U ldali_admin -d ldali_dwh -c "
SELECT relname, n_live_tup FROM pg_stat_user_tables
WHERE schemaname='raw'
AND (relname LIKE '%dvizhenie%' OR relname LIKE '%putevoy%');"
```

### Шаг 3 — Если dvizhenie/putevoy пустые — перезапустить
```bash
docker exec -it ldali-airflow-web airflow dags trigger dag_extract_dvizhenie
docker exec -it ldali-airflow-web airflow dags trigger dag_extract_putevoy_list
```

### Шаг 4 — После заполнения raw — запустить transform
```bash
docker exec -it ldali-airflow-web airflow dags trigger dag_transform_mart
```

### Шаг 5 — Финальная проверка mart
```bash
docker exec -it ldali-postgres-dwh psql -U ldali_admin -d ldali_dwh -c "
SELECT schemaname, relname AS tablename, n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname IN ('raw','mart') AND n_live_tup > 0
ORDER BY schemaname, rows DESC;"
```

### Шаг 6 — Проверка дублей (идемпотентность)
```bash
# Запустить DAG повторно, потом:
docker exec -it ldali-postgres-dwh psql -U ldali_admin -d ldali_dwh -c "
SELECT doc_id, pole_id, nomenklatura_id, COUNT(*)
FROM mart.fact_vypusk_urozhaya
GROUP BY doc_id, pole_id, nomenklatura_id
HAVING COUNT(*) > 1;"
```
Результат должен быть 0 строк.

---

## 📸 СКРИНШОТЫ СОБРАННЫЕ СЕГОДНЯ
1. Airflow DAGs — все 4 активны, Running 4, Failed 0
2. Connections — odata_1c_ld и postgres_dwh настроены
3. Variables — odata_1c_base_url заполнен
4. Терминал — raw таблицы с данными из 1С
5. dim_date — 5 строк с agro_phase, season_label на русском
6. Airflow — triggered dag_transform_mart баннер

---

## 🔑 УЧЁТНЫЕ ДАННЫЕ
- Airflow UI: http://localhost:8080 | admin / admin
- pgAdmin: http://localhost:5050 | admin@admin.com / admin
- PostgreSQL: ldali-postgres-dwh:5432 | ldali_admin / ldali_admin_change_me
- 1С OData: http://10.50.254.22/ld_erp_ibOdata/odata/standard.odata | OdataBi / xxx123
