# Сессия 2026-09-07 — трубопровод заправочных ведомостей, витрины топлива и сравнение с нормой

Репозиторий: https://github.com/SexyIdol2003/LesDaliBI
Фиксация контекста для следующей сессии. Связанные коммиты в `main`: `b74124`…`b190aa0`.

## Участвовавшие системы
- Postgres DWH в Docker: `ldali-postgres-dwh`, база `ldali_dwh`, роль `ldali_admin`.
- Airflow в Docker: `ldali-airflow-scheduler` / `ldali-airflow-webserver`.
- Схемы: `raw` (сырые данные 1С OData / справочники), `mart` (витрины для BI/DataLens со звездной схемой: `dim_date`, `dim_equipment`, `dim_field`).
- BI-роль доступа: `datalens_ro` (read-only на `mart`).

## 1. Загрузка заправочных ведомостей (`dag_extract_zapravochnaya_vedomost`)

Созданы таблицы `raw.r1c_zapravochnaya_vedomost` (шапка документа) и `raw.r1c_zapravochnaya_vedomost_gsm` (табличная часть ГСМ) и DAG их загрузки из OData-сущности `Document_АпкЗаправочныеВедомости`.

Найдены и исправлены два бага:
1. **HTTP 501 Not Implemented** на `$expand=ГСМ`. В этом OData-сервисе `$expand` для табличных частей не работает. Заценил на `$select`, включающий имя таблицной части "ГСМ" как обычное поле — табличная часть пришла инлайн.
2. **`psycopg2.errors.InvalidDatetimeFormat`** на загрузке строк. 1С возвращает время как ISO-datetime с фиктивной датой (`0001-01-01T07:00:00`), а колонка — тип `time`. Добавлена функция `_extract_time()`, отрезающая часть до `T`.

Файл DAG: `files/airflow/dags/dag_extract_zapravochnaya_vedomost.py`. Результат после фикса: **616 документов**, **3843 строки ГСМ**. Расписание: ежедневно в 01:45.

## 2. Витрина `mart.fact_fuel_refuel`

Важно: таблица `mart.fact_fuel_usage` (с `field_sk`, `hours`) **уже существовала** из более раннего плана (03_mart.sql/05_mart_facts.sql) и пуста — её не тронули. Для заправок правильной таргетом оказалась **`mart.fact_fuel_refuel`** (`date_day`, `equipment_sk`, `fuel_brand_id`, `liters`, `src_doc_ref`) — также существовала, но была пуста.

Скрипт загрузки: `files/17_mart_fact_fuel_refuel_load.sql` (TRUNCATE + INSERT, полная перезагрузка). Джойны на справочники через `::text`, потому что в `raw.r1c_zapravochnaya_vedomost` все FK — `uuid`, а в `raw.r1c_equipment._id` — `text`.

**Автоматизация**: логика перезагрузки встроена как отдельный таск `load_mart_fuel_refuel` в `dag_extract_zapravochnaya_vedomost.py`, зависимый от `load_zapr_docs`/`load_zapr_lines`. Внутри также идемпотентный `GRANT SELECT ... TO datalens_ro` (выдаётся при каждом загрузке).

Результат: **3805 строк**, **619 375.048 л** суммарно (38 строк отсеялись корректно — непроведённые/удалённые документы). `equipment_sk IS NULL` = 0, т.е. вся техника растознана корректно через `dim_equipment.code_1c`.

Важный артефакт: `files/16_mart_fact_fuel_usage.sql` — первая подага построить витрину как VIEW с таким же именем — ошибочная, содержит только `SELECT 1;` после реверта. Не использовать.

## 3. Скрытый watermark-баг в `dag_extract_putevoy_list`

При попытке сравнить факт с нормой выяснилось: во всей `raw.r1c_putevoy_list_lines` (25285 строк на тот момент) поля `chasov`, `mototochas`, `gektarov`, `normativny_raskhod_gsm` были **100% NULL**, хотя код их корректно читает из OData.

Причина: `_extract_docs()` вычисляет `dt_from` не из `execution_date` Airflow, а из `context["dag_run"].conf.get("date_from")`, и если не задано — из `datetime.utcnow() - lookback_hours` (по умолчанию 48 часов). Фильтр к OData — `Date ge datetime'{dt_from}'` (открытый, без верхней границы). Документы в базе датированы 2021–2026-07/08, а обычный ежедневный забор с lookback в пару суток никогда не затрагивал исторические данные с новыми полями (поля ГСМ/выработки были добавлены в `LINE_SELECT` раньше сегодня, до нашего вмешательства).

**Фикс**: ручный триггер с `--conf`:
```bash
docker exec -i ldali-airflow-scheduler airflow dags trigger \
  --conf '{"date_from": "2021-01-01T00:00:00"}' \
  dag_extract_putevoy_list
```
Обычный `airflow tasks clear`/`dags trigger` без `--conf` тут **не помогает** — без `date_from` в conf всегда берётся lookback от текущего момента.

Результат после backfill: **25696 строк**, `chasov` заполнен на 100%, `gektarov` — 7683/25696 (30%), `normativny_raskhod_gsm` — 17049/25696 (66%), `mototochas` на уровне строк почти не ведётся (2 строки), вместо него в 1С используется `МоточасовВсего` на уровне шапки документа (тоже почти не заполнено — 2 из 12395).

**Важно на будущее**: в `dag_extract_zapravochnaya_vedomost.py` такого watermark-механизма нет (он всегда забирает все данные целиком без фильтра даты), но **стоит проверить остальные DAG'и** (`dag_extract_catalogs`, `dag_extract_catalogs2`, `dag_extract_dvizhenie`, `dag_extract_field_writeoffs`, `dag_extract_fuel_summary`, `dag_extract_tok_vzveshivanie`, `dag_extract_work_types`) на аналогичный watermark/lookback-баг — он мог быть скопирован в них тоже.

## 4. Витрина сравнения факт/норма (`mart.fact_fuel_norm_variance`)

Раскрытие: справочник `raw.r1c_operation_fuel_norms` **пуст (0 строк)**, но это не помешало — норма уже рассчитана на уровне строки `raw.r1c_putevoy_list_lines.normativny_raskhod_gsm`.

Скрипт: `files/19_mart_fact_fuel_norm_variance.sql` (чистая VIEW, без конфликтов с таблицами). Гранулярность: **месяц + техника** (не день, т.к. заправка ≠ моментальный расход, в баке есть запас). Сравнивает `SUM(liters)` из `mart.fact_fuel_refuel` с `SUM(normativny_raskhod_gsm)` из `putevoy_list_lines` по `dim_equipment.eq_sk`.

Периоды и покрытие техники между источниками совместимы: 2021-07 – 2026-07/08, 22–24 ед. техники.

### Найденные аномалии (требуют расследования, не разобраны в этой сессии)
| Техника | Месяц | Факт, л | Норма, л | Отклонение |
|---|---|---|---|---|
| Valtra T190, №1879ХР50 | 2026-07 | 510.4 | 114.0 | +348% |
| Lovol 2604, №9131ХХ | 2026-01 | 157.0 | 45.0 | +249% |
| Lovol 2604, №9130ХХ | 2026-06 | 2307.6 | 708.3 | +226% |

Остальная техника в диапазоне +5%…+36% — в пределах ожидаемой погрешности "залив ≠ расход в тот же месяц".

## Состояние ключевых таблиц на конец сессии
| таблица | строк | комментарий |
|---|---|---|
| raw.r1c_zapravochnaya_vedomost | 616 | готово, авто-обновление |
| raw.r1c_zapravochnaya_vedomost_gsm | 3843 | готово |
| raw.r1c_putevoy_list | 12395 | дозагружено полностью (backfill с 2021-01-01) |
| raw.r1c_putevoy_list_lines | 25696 | chasov=100%, gektarov=30%, normativny_raskhod_gsm=66% |
| raw.r1c_operation_fuel_norms | 0 | пуст, не используется (норма уже в putevoy_list_lines) |
| mart.fact_fuel_refuel | 3805 | авто-перезагрузка внутри dag_extract_zapravochnaya_vedomost |
| mart.fact_fuel_usage | 0 | пустая таблица-заготовка, не тронута, не наполняли |
| mart.fact_fuel_norm_variance | — | VIEW, работает |

## Артефакты, добавленные в `files/`
- `14_raw_zapravochnaya_vedomost.sql` — DDL raw-таблиц заправочных ведомостей (создано раньше сегодняшней сессии).
- `16_mart_fact_fuel_usage.sql` — **Не использовать**, ошибочная попытка, ревертнута.
- `17_mart_fact_fuel_refuel_load.sql` — ручной ETL для fact_fuel_refuel (та же логика, что в DAG).
- `18_grant_fact_fuel_refuel.sql` — разовый GRANT (также есть в DAG).
- `19_mart_fact_fuel_norm_variance.sql` — витрина сравнения, актуальная.
- `files/airflow/dags/dag_extract_zapravochnaya_vedomost.py` — обновлён, теперь включает таск `load_mart_fuel_refuel`.

## Открытые вопросы для следующей сессии
1. **Расследовать аномалии** из `mart.fact_fuel_norm_variance` (Valtra T190 №1879ХР50 июль, Lovol 2604 №9131ХХ январь, Lovol 2604 №9130ХХ июнь) — нет ли пропусков путевых листов за те месяцы.
2. **Проверить другие DAG'и на watermark-баг** аналогично `dag_extract_putevoy_list` (см. раздел 3).
3. Поле `mototochas`/`МоточасовВсего` почти не ведётся в 1С — стоит уточнить у заказчика, нужен ли этот показатель вообще, или ориентироваться только на `chasov`/`gektarov`.
4. `probeg_km`, `toplivo_vydano`, `toplivo_vozvrat` в `raw.r1c_putevoy_list` — поля в DDL есть, но не существуют как простые поля в этой сущности 1С (подтверждено в коде DAG комментарием "не существуют") — остаются пустыми, это не баг.
5. `mart.fact_fuel_usage` (старая таблица с `field_sk`/`hours`) остаётся пустой — решить, нужна ли она вообще, или удалить.
