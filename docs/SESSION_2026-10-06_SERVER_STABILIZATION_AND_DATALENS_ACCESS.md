# SESSION 2026-10-06 — Стабилизация сервера vm-bi, автозапуск, доступ сотрудников к DataLens

Дата: 2026-10-06 (вечер, МСК)
Сервер: `vm-bi` (Ubuntu 22.04.5 LTS, `<server-ip>`, пользователь `ilya`)
Репозиторий: https://github.com/SexyIdol2003/LesDaliBI

---

## 1. Цель сессии

1. Войти в контекст проекта (DWH «Лесные дали», Airflow, dbt, DataLens) и понять, что работает на сервере.
2. Сделать так, чтобы сервисы (DWH, Airflow, pgAdmin, DataLens) автоматически поднимались после падения и перезагрузки.
3. Выгрузить с сервера в GitHub то, чего там нет.
4. Дать сотрудникам доступ к DataLens с разграничением ролей viewer / editor.

---

## 2. Исходное состояние

### Структура на сервере
- `/data/apps/LesDaliBI/files` — DWH (Postgres), Airflow, pgAdmin, dbt (`docker-compose.yml`).
- `/data/apps/datalens` — DataLens (`docker-compose.production.yaml`, собран `init.sh`).
- Диски: `/` — 10 ГБ (LV `ubuntu-vg/ubuntu-lv`), `/data` — 80 ГБ (LV `vg-data/data`, занято ~13 ГБ).
- Доступ с ноутбука — SSH-туннель:
  `ssh -N -L 18088:localhost:8088 -L 18080:localhost:8080 -L 15050:localhost:5050 ilya@<server-ip>`
  (DataLens :18088, Airflow :18080, pgAdmin :15050).

### Контекст в репозитории (по структуре и коммитам)
- Файлы `files/01…77_*.sql` — схемы raw / staging / mart / meta, витрины и вьюхи.
- Последние сессии (см. `SESSION_*.md`, `docs/`): KPI5 (распределение топлива по полям), KPI6 (затраты на га за сезон), DataLens, деплой на сервер (`docs/session_2026-10-02_server_datalens_airflow.md`).
- Шесть витрин-показателей: `mart.fact_fuel_plan_fact_by_operation`, `mart.fact_fuel_norm_variance`, `mart.fact_agro_input_usage`, `mart.v_fact_harvest_yield_by_variety`, `mart.v_kpi5_fuel_field_allocation_scenario`, `mart.v_field_season_kpi6_with_scenarios`.

---

## 3. Что сделали

### 3.1 Автозапуск и политика перезапуска

- Включён автозапуск Docker и containerd:
  `sudo systemctl enable --now docker containerd` (статус: enabled).
- Проверены политики restart. **Находка:** `ldali-postgres-dwh` и `ldali-postgres-airflow` имели `no` (после ребута не поднялись бы, Airflow остался бы без БД). У остальных уже было `unless-stopped`.
- Всем контейнерам выставлено `unless-stopped`:
  `docker ps -aq | xargs -r docker update --restart unless-stopped`.
- Побочный эффект: `ldali-airflow-init` (одноразовый) тоже получил `unless-stopped`. Вернуть можно командой `docker update --restart no ldali-airflow-init`.
- В compose-файлах политика прописана (резервные копии: `files/docker-compose.yml.before-restart-policy`, `docker-compose.production.yaml.before-restart-policy`).

### 3.2 Настройки Docker daemon

Итоговый `/etc/docker/daemon.json`:

```json
{
  "data-root": "/data/docker",
  "live-restore": true,
  "log-driver": "json-file",
  "log-opts": { "max-size": "10m", "max-file": "5" }
}
```

- `live-restore` — контейнеры живут при перезапуске демона Docker.
- Ротация логов (10 МБ × 5 файлов) защищает диск. Применяется только к контейнерам, созданным после рестарта (нужен `compose up -d --force-recreate` в окно обслуживания).

### 3.3 Инцидент: потеря `data-root` (обнаружен и устранён)

**Что случилось.** Я (по рекомендации) перезаписал `/etc/docker/daemon.json` командой `tee`. В старом файле, судя по результату, был `"data-root": "/data/docker"`. После рестарта Docker начал работать из `/var/lib/docker`:
- пропали контейнеры и тома DataLens (15 контейнеров → 6);
- подняты старые остатки `ldali-*` с пустой БД (дамп `pre_move.dump` = 4 КБ).

**Диагностика.**
- `docker info` → `Docker Root Dir: /var/lib/docker`;
- `/data/docker` содержал полный набор (`containers`, `volumes`, `engine-id`…);
- `/var/lib/containerd` смонтирован как bind из `/data/containerd` (образы уже на `/data`).

**Исправление.**
1. `docker stop $(docker ps -q)` (остановить контейнеры старого корня).
2. В `daemon.json` возвращён `"data-root": "/data/docker"` + сохранены `live-restore` и `log-opts`.
3. `sudo systemctl restart docker`.

**Результат.** `Docker Root Dir: /data/docker`, `Live Restore Enabled: true`, 14 контейнеров в `Up` (8 DataLens + postgres DataLens + 5 ldali). Тома: `datalens_db-postgres`, `files_pgadmin-data`, `files_postgres-airflow-data`, `files_postgres-dwh-data`.
Проверка данных DWH: `mart.fact_fuel_norm_variance` — 778 строк; БД `ldali_dwh` — 252 МБ; схемы `mart, meta, public, raw, staging`.

**Урок.** Перед перезаписью конфигов всегда делать `cp` и `cat`, а не `tee` поверх существующего файла.

### 3.4 Диск и бэкап

- LV корня расширен на всё свободное место VG: `sudo lvextend -r -l +100%FREE /dev/mapper/ubuntu--vg-ubuntu--lv` (10 ГБ → ~14,25 ГБ).
- Создан `/data/backups`, снят дамп DWH: `/data/backups/ldali_dwh_2026-10-06.dump` (9,8 МБ, формат `-Fc`).
- Ошибочный пустой `pre_move.dump` удалён.
- Установлен ночной бэкап в cron (`pg_dump` в 02:30, хранение 7 дней) и проверка `docker compose up -d` каждые 5 минут для обеих папок.
- Старый `/var/lib/docker` (~5 ГБ, устаревшие остатки) не удалён; план: переименовать через неделю и удалить.

### 3.5 Выгрузка в GitHub

- На сервере в `/data/apps/LesDaliBI` были изменения и неотслеживаемые файлы: `files/docker-compose.yml` (M), `files/docker-compose.yml.before-restart-policy`, `files/init-db/`, `files/pgadmin/`, `import/`, `backups/`.
- Обновлён `.gitignore`: `.env`, `*.dump`, `*.dump.gz`, `airflow/logs/`, `__pycache__/`, `*.pyc`, `*.bak`.
- Добавлены: `deploy/datalens/` (compose + `.env.example`), `deploy/server/README.md` (daemon.json и crontab).
- Коммит `d1d65aa` → после rebase запушен как `ee43ef9` в `main`.

> ⚠ **Инцидент безопасности (не закрыт).** В коммит попал `deploy/datalens/docker-compose.production.yaml` с реальными секретами: пароль админа DataLens, мастер-токены, пароль Postgres DataLens, приватные ключи, ключи шифрования. Репозиторий **публичный** (`private: false`). Рекомендации (rotate секретов, сделать репозиторий приватным, убрать коммит из истории) озвучены; пользователь решил оставить как есть. Риск принят, секреты считаются скомпрометированными.
> Также в коммите: `files/init-db/01_schemas.sql`, `files/pgadmin/servers.json`, `import/Лесные Дали — BI.json` (возможны пароли и параметры подключений).

- Создан SSH-ключ `~/.ssh/id_ed25519_github` (публичный ключ в GitHub не добавлен; push прошёл по существующей авторизации). Remote переключён на SSH: `git@github.com:SexyIdol2003/LesDaliBI.git`.

### 3.6 Доступ сотрудников к DataLens

- Аутентификация включена (`AUTH_ENABLED: "true"`, контейнер `datalens-auth`); `UI_APP_ENDPOINT: ""`, `DISABLE_WILDCARD_COOKIE: "true"` — вход по IP работает.
- Роли (глобальные, без прав на отдельные дашборды): `datalens.viewer` (просмотр), `datalens.editor` (создание/правка), `datalens.admin`.
- В админке созданы пользователи и роли (в т.ч. viewer-аккаунт руководителя).
- Проверка: `curl -sI http://<server-ip>:8088` → `HTTP/1.1 200 OK`; вход под viewer из режима инкогнито без туннеля — успешен.
- Проверка портов: наружу опубликован только `0.0.0.0:8088` (DataLens UI). Airflow `127.0.0.1:8080`, pgAdmin `127.0.0.1:5050`, Postgres DWH `127.0.0.1:5432` — только локально (доступ по SSH-туннелю).
- Раздача: ссылка `http://<server-ip>:8088`, личный логин и пароль, роль viewer; работает из офисной сети или через VPN.

---

## 4. Итоговое состояние

| Компонент | Статус |
|---|---|
| Docker автозапуск | enabled |
| Restart policy | `unless-stopped` у всех контейнеров |
| `data-root` | `/data/docker` |
| `live-restore` | включён |
| Ротация логов | настроена (для новых контейнеров) |
| Корень `/` | ~14,25 ГБ |
| Бэкап DWH | ночной cron, 7 дней, `/data/backups` |
| DataLens | доступен по `http://<server-ip>:8088`, роли viewer/editor |
| Airflow / pgAdmin / DWH | только localhost + SSH-туннель |
| GitHub | `ee43ef9` в `main` (содержит секреты, см. 3.5) |

---

## 5. Открытые задачи

1. **Секреты.** Сменить пароли и ключи DataLens, пароль админа, пароли ролей БД; сделать репозиторий приватным; убрать секреты из истории (`git reset` + `--force-with-lease` или `git filter-repo`). Заменить значения в compose на `${...}`, секреты держать в `.env`.
2. **Тест автозапуска.** Выполнить `sudo reboot` и убедиться, что все 14 контейнеров вернулись в `Up`; `docker kill ldali-pgadmin` — проверить подъём отдельного контейнера.
3. **Healthcheck и autoheal.** Добавить healthcheck для pgAdmin, Airflow, DataLens и контейнер `willfarrell/autoheal`, чтобы перезапускались «зависшие» контейнеры.
4. **Пересоздание контейнеров** (`up -d --force-recreate`) в окно обслуживания, чтобы применилась ротация логов.
5. **Внешняя копия бэкапов** (Google Drive или другой сервер): бэкапы сейчас на том же диске `/data`.
6. **HTTPS и ограничение подсети** для порта 8088 (`DOCKER-USER`, обратный прокси), если доступ выйдет за пределы внутренней сети.
7. **Образ Airflow.** Ошибка `snapshotter.Usage … no such file` на слое `kubernetes/…pyc` — пересобрать или заново скачать образ `apache/airflow:2.10.3-python3.11`.
8. **Старый `/var/lib/docker`** переименовать и удалить после недели стабильной работы.
9. **Обновления ОС:** 84 пакета (14 security) — установить в окно обслуживания и перезагрузиться.
10. **Очистка репозитория:** `*.before-restart-policy` лучше убрать из Git (держать только `.gitignore`).

---

## 6. Полезные команды

```bash
# Состояние
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
docker info | grep -E "Docker Root Dir|Live Restore"
df -h / /data

# Политики restart
docker ps -aq | xargs docker inspect -f '{{.Name}} -> {{.HostConfig.RestartPolicy.Name}}'

# Бэкап DWH
docker exec ldali-postgres-dwh pg_dump -U ldali_admin -Fc ldali_dwh > /data/backups/ldali_dwh_$(date +%F).dump

# Поднять всё
cd /data/apps/LesDaliBI/files && docker compose up -d
cd /data/apps/datalens && docker compose -f docker-compose.production.yaml up -d

# Туннель с ноутбука (БЕЗ знака %!)
ssh -N -L 18088:localhost:8088 -L 18080:localhost:8080 -L 15050:localhost:5050 ilya@<server-ip>
```
