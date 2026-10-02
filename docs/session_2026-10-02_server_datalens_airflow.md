# LesDali BI — серверный контур и итоги сессии

**Дата:** 2 октября 2026  
**Сервер:** `vm-bi` / Ubuntu 22.04.5 LTS  
**Назначение:** единый сервер DWH, загрузок Airflow и DataLens Community Edition для «Лесных Далей».

> **Важно о секретах:** этот документ специально **не содержит пароли**, токены и содержимое `.env`. Пароли хранятся только в защищённых конфигурациях на сервере или у администратора. Не добавлять `.env` и этот документ с любыми вписанными секретами в публичный Git-репозиторий.

---

## 1. Что теперь работает

На VM развернуты два независимых Docker Compose-стека:

1. **DWH / ETL-стек** в `/data/apps/LesDaliBI/files`:
   - PostgreSQL DWH;
   - PostgreSQL для метаданных Airflow;
   - Apache Airflow Webserver;
   - Apache Airflow Scheduler;
   - pgAdmin.
2. **DataLens Community Edition** в `/data/apps/datalens`:
   - интерфейс DataLens;
   - сервис аутентификации;
   - DataLens Data API и Control API;
   - DataLens PostgreSQL;
   - Temporal;
   - вспомогательные сервисы DataLens.

В DataLens был импортирован рабочий воркбук «Лесные Дали — BI» с Mac. В его составе сохранены структура подключений, datasets, charts и dashboards. После импорта пароль PostgreSQL требуется задавать повторно — это нормальное безопасное поведение экспорта DataLens.

---

## 2. Сервер и пути

| Параметр | Значение |
|---|---|
| Имя VM | `vm-bi` |
| ОС | Ubuntu 22.04.5 LTS |
| IP во внутренней сети | `10.50.254.42` |
| Пользователь SSH | `ilya` |
| DWH-проект | `/data/apps/LesDaliBI/files` |
| DataLens | `/data/apps/datalens` |
| Каталог импортов BI | `/data/apps/LesDaliBI/import` |
| Диск приложения/данных | `/data` |
| Docker | включён в автозапуск systemd |

Текущая VM находится во внутренней сети. Административные сервисы не должны публиковаться напрямую в интернет.

---

## 3. Сервисы и порты

### Внутри VM

| Сервис | Контейнер / сервис | Состояние | Привязка порта на VM | Назначение |
|---|---|---|---|---|
| PostgreSQL DWH | `ldali-postgres-dwh` / `postgres-dwh` | healthy | `127.0.0.1:5432` | DWH, схемы `raw`, `staging`, `mart`, `meta` |
| PostgreSQL Airflow | `ldali-postgres-airflow` / `postgres-airflow` | healthy | только Docker-сеть | служебная БД Airflow |
| Airflow Webserver | `ldali-airflow-web` / `airflow-webserver` | Up | `127.0.0.1:8080` | UI Airflow |
| Airflow Scheduler | `ldali-airflow-scheduler` / `airflow-scheduler` | Up | только Docker-сеть | расписание и выполнение DAG |
| pgAdmin | `ldali-pgadmin` / `pgadmin` | Up | `127.0.0.1:5050` | администрирование PostgreSQL |
| DataLens UI | `datalens-ui` / `ui` | Up | `0.0.0.0:8088` | интерфейс дашбордов |
| DataLens PostgreSQL | `datalens-postgres` / `postgres` | healthy | только Docker-сеть | внутренние метаданные DataLens |
| Temporal | `datalens-temporal` / `temporal` | healthy | только Docker-сеть | фоновые процессы DataLens |

### Почему административные сервисы закрыты

PostgreSQL DWH, Airflow и pgAdmin привязаны к `127.0.0.1`: их нельзя открыть с другого компьютера напрямую. Это преднамеренная защита. Для администратора доступ обеспечивается только через SSH-туннель.

DataLens временно опубликован на `0.0.0.0:8088`, то есть может быть доступен в сети VM. До выдачи доступа сотрудникам необходимо настроить контролируемую публикацию: домен, HTTPS и аутентификацию/ограничение сети. Не следует открывать Airflow, pgAdmin или PostgreSQL наружу.

---

## 4. Вход для администратора

### SSH на сервер

На Mac или другом администраторском компьютере:

```bash
ssh ilya@10.50.254.42
```

### SSH-туннель для закрытых сервисов

На **локальном компьютере администратора**, в отдельном окне Terminal:

```bash
ssh -N \
  -L 18088:127.0.0.1:8088 \
  -L 18080:127.0.0.1:8080 \
  -L 15050:127.0.0.1:5050 \
  ilya@10.50.254.42
```

Окно не закрывать, пока нужен доступ. Остановка туннеля: `Ctrl+C`.

После запуска туннеля открывать **на компьютере администратора**:

| Интерфейс | URL |
|---|---|
| DataLens на сервере | `http://localhost:18088` |
| Airflow на сервере | `http://localhost:18080` |
| pgAdmin на сервере | `http://localhost:15050/browser/` |

Если DataLens на Mac тоже запущен на `8088`, серверный DataLens всегда открывать на `18088`, чтобы не перепутать установки.

### Прямой просмотр внутри VM

На самой VM доступны:

```text
http://127.0.0.1:8088      # DataLens
http://127.0.0.1:8080      # Airflow
http://127.0.0.1:5050/browser/  # pgAdmin
```

### Учётные записи и пароли

| Система | Логин | Где взять пароль / примечание |
|---|---|---|
| DataLens server | `admin` | Секрет `AUTH_ADMIN_PASSWORD` в `/data/apps/datalens/.env`; не выводить и не пересылать целиком |
| PostgreSQL DWH для BI | `datalens_ro` | Отдельный пароль PostgreSQL, установленный администратором на сервере в этой сессии |
| PostgreSQL DWH admin | `ldali_admin` | Использовать только для обслуживания; пароль в `/data/apps/LesDaliBI/files/.env` |
| Airflow | существующая учётная запись Airflow | Проверять/сбрасывать только по необходимости; пароль не хранить в документации |
| pgAdmin | существующая учётная запись pgAdmin | Пароль не хранить в документации |

#### Смена пароля `datalens_ro`

На VM пароль роли можно безопасно изменить без показа его в истории shell:

```bash
cd /data/apps/LesDaliBI/files

docker compose exec postgres-dwh \
  psql -U ldali_admin -d ldali_dwh \
  -c '\\password datalens_ro'
```

Терминал дважды запросит новый пароль. Затем такой же пароль следует сохранить в подключении DataLens.

---

## 5. Подключение DataLens к DWH

В DataLens использовать PostgreSQL-подключение со значениями:

| Поле | Значение |
|---|---|
| Хост | `postgres-dwh` |
| Порт | `5432` |
| База данных | `ldali_dwh` |
| Пользователь | `datalens_ro` |
| Пароль | действующий пароль роли `datalens_ro` |
| SSL | выключен для внутренней Docker-сети |
| Уровень SQL-запросов | по умолчанию выключен |

Не использовать `localhost` как hostname в настройках DataLens: внутри контейнера `localhost` означает сам контейнер DataLens, а не DWH. Для BI предназначена роль `datalens_ro`, которая имеет read-only доступ к витринам `mart`.

После импорта воркбука:

1. Открыть импортированное PostgreSQL-подключение;
2. Задать/обновить хост, БД, пользователя и пароль;
3. Нажать «Проверить подключение»;
4. Сохранить подключение;
5. На дашбордах нажать `Retry` или обновить страницу.

Если на chart отображается `Source error / Database error`, в первую очередь проверять сохранённое PostgreSQL-подключение и доступ DataLens к сети DWH.

---

## 6. Сети Docker

DWH Compose-сеть называется `files_default`.

Проверить участников:

```bash
docker network inspect files_default \
  --format '{{range .Containers}}{{.Name}}{{"\\n"}}{{end}}'
```

Для работы DataLens с DWH в этой сети должны быть как минимум:

```text
ldali-postgres-dwh
datalens-data-api
datalens-control-api
```

Если нужные контейнеры DataLens отсутствуют, подключать их к сети только после проверки актуальных имён контейнеров:

```bash
docker network connect files_default datalens-data-api
docker network connect files_default datalens-control-api
```

Перед повторным выполнением `docker network connect` убедиться, что контейнер уже не подключён, иначе Docker вернёт сообщение о существующем endpoint.

---

## 7. Автозапуск и самовосстановление

### Docker daemon

Docker включён в автозапуск:

```bash
sudo systemctl is-enabled docker
# ожидается: enabled
```

### Основной BI-стек

Создан systemd-unit:

```text
lesdali-bi.service
```

Он запускает `/data/apps/LesDaliBI/files`:

```bash
docker compose up -d --remove-orphans
```

В нём поднимаются DWH PostgreSQL, PostgreSQL Airflow, Airflow, pgAdmin и инициализационные зависимости.

Проверка:

```bash
sudo systemctl status lesdali-bi.service --no-pager -l
```

Корректный финальный статус:

```text
Active: active (exited)
```

Это нормально, потому что unit использует `Type=oneshot` и запускает контейнеры в фоне.

### DataLens

Создан systemd-unit:

```text
datalens.service
```

Конфигурация DataLens:

```text
/data/apps/datalens/docker-compose.production.yaml
```

Постоянным сервисам DataLens добавлена политика:

```yaml
restart: unless-stopped
```

Она установлена для:

- `auth`;
- `control-api`;
- `data-api`;
- `meta-manager`;
- `postgres`;
- `temporal`;
- `ui`;
- `ui-api`;
- `us`.

Unit запускает:

```bash
docker compose -f docker-compose.production.yaml up -d --remove-orphans
```

Проверка:

```bash
sudo systemctl status datalens.service --no-pager -l
cd /data/apps/datalens
docker compose -f docker-compose.production.yaml ps
```

Ожидаемый статус systemd:

```text
Active: active (exited)
```

Ожидаемые healthcheck:

```text
datalens-postgres  Up ... (healthy)
datalens-temporal  Up ... (healthy)
```

### Что даёт эта настройка

После перезагрузки VM:

```text
Ubuntu
  → Docker daemon
  → lesdali-bi.service
      → DWH + Airflow + pgAdmin
  → datalens.service
      → DataLens Community Edition
```

Если постоянный DataLens-контейнер аварийно завершится, Docker попробует автоматически вернуть его по `unless-stopped`. Это не заменяет мониторинг, резервное копирование, обновления или устранение ошибок данных/конфигурации.

---

## 8. Рабочие команды администратора

### Состояние DWH и Airflow

```bash
cd /data/apps/LesDaliBI/files
docker compose ps
sudo systemctl is-active lesdali-bi.service
```

### Состояние DataLens

```bash
cd /data/apps/datalens
docker compose -f docker-compose.production.yaml ps
sudo systemctl is-active datalens.service
```

### Логи DataLens UI

```bash
cd /data/apps/datalens
docker compose -f docker-compose.production.yaml logs --tail=100 ui
```

### Логи DataLens API

```bash
cd /data/apps/datalens
docker compose -f docker-compose.production.yaml logs --tail=100 data-api control-api
```

### Логи Airflow

```bash
cd /data/apps/LesDaliBI/files
docker compose logs --tail=100 airflow-webserver airflow-scheduler
```

### Проверка локальной доступности интерфейсов на VM

```bash
curl -I http://127.0.0.1:5050/browser/
curl -I http://127.0.0.1:8080/
curl -I http://127.0.0.1:8088/
```

Допустимые ответы `200`, `302` или `401` означают, что веб-сервис отвечает; дальнейшая авторизация зависит от приложения.

### Проверка диска и Docker

```bash
df -h / /data
docker system df
```

Не выполнять без отдельной проверки:

```bash
docker compose down -v
docker system prune -a --volumes
```

Эти команды могут удалить Docker volumes и вместе с ними данные PostgreSQL/DataLens.

---

## 9. Резервные копии и восстановление

### Резервная копия конфигурации DataLens

Перед добавлением restart policy создан файл:

```text
/data/apps/datalens/docker-compose.production.yaml.before-restart-policy
```

### Экспорт воркбука

Рабочий воркбук был экспортирован с Mac как:

```text
Лесные Дали — BI.json
```

и передан на VM в каталог:

```text
/data/apps/LesDaliBI/import/
```

Экспорт DataLens удобно использовать как резервную копию структуры воркбука. Он переносит объекты BI, но пароль источника данных необходимо вводить повторно после импорта.

### Что нужно добавить дальше

Для полноценной эксплуатационной готовности следует:

1. Настроить регулярный backup PostgreSQL DWH и PostgreSQL DataLens на `/data` или внешний защищённый storage.
2. Настроить ограничение роста Docker logs, потому что корневой раздел `/` небольшой.
3. Проверять свободное место и статусы контейнеров.
4. Зафиксировать известные пароли в корпоративном password manager, а не в текстовом Markdown-файле.

---

## 10. Публикация дашбордов сотрудникам

Текущий SSH-туннель подходит для администратора, но не для массового просмотра дашбордов.

Рекомендуемый следующий этап:

1. Выделить DNS-имя, например `bi.<ваш-домен>`.
2. Поставить Nginx или другой reverse proxy перед DataLens.
3. Выпустить TLS-сертификат HTTPS.
4. Настроить DataLens-аутентификацию, права пользователей/коллекций либо ограничение по VPN/корпоративной сети.
5. Публиковать наружу **только DataLens**.
6. Не публиковать наружу `5432`, `5050` и `8080`.

До этого этапа не выполнять:

```bash
sudo ufw allow 5432
sudo ufw allow 5050
sudo ufw allow 8080
```

Порты PostgreSQL, pgAdmin и Airflow остаются административными и должны быть доступны только по SSH-туннелю или через VPN.

---

## 11. Краткая итоговая схема

```text
1С / Google Sheets / иные источники
             │
             ▼
       Airflow DAGs
             │
             ▼
PostgreSQL DWH: raw → staging → mart
             │
             │  read-only: datalens_ro
             ▼
DataLens Community Edition
             │
             ▼
Дашборды «Лесные Дали»
             │
             ├─ Администратор: SSH-туннель
             └─ Сотрудники: следующий этап — HTTPS / домен / авторизация
```

---

## 12. Быстрый чек-лист после reboot

На VM:

```bash
sudo systemctl is-active docker
sudo systemctl is-active lesdali-bi.service
sudo systemctl is-active datalens.service

cd /data/apps/LesDaliBI/files
docker compose ps

cd /data/apps/datalens
docker compose -f docker-compose.production.yaml ps
```

Ожидается:

```text
docker: active
lesdali-bi.service: active
datalens.service: active
ldali-postgres-dwh: Up (healthy)
ldali-postgres-airflow: Up (healthy)
datalens-postgres: Up (healthy)
datalens-temporal: Up (healthy)
datalens-ui: Up
```
