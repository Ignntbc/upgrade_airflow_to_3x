# Airflow 2.7.3 → 3.2.1. Руководство по миграции

> Документ построен в соответствии с
> [официальным руководством Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html)
> и адаптирован под наш стенд: `docker-compose.yml`, кастомный `Dockerfile`,
> CeleryExecutor + RabbitMQ + Postgres.
>
> Airflow 3.x — мажорный релиз с принципиальными изменениями
> архитектуры, API, схемы БД и DAG SDK. Прямой апгрейд поддерживается
> **с любой версии 2.7+**, поэтому идём с 2.7.3 сразу на 3.2.1
> (промежуточный шаг через 2.10.x не требуется).

---

## 0. TL;DR (краткий план по официальному гайду)

Восемь обязательных шагов:

1. **Шаг 1** — Предусловия: проверить версию Airflow (≥ 2.7), Python и отсутствие удалённых фич.
2. **Шаг 2** — Очистить (`airflow db clean`) и **снять полный бэкап** метабазы.
3. **Шаг 3** — Проверить DAG'и `ruff check --select AIR301,AIR302,AIR311,AIR312` и поправить нарушения.
4. **Шаг 4** — Установить `apache-airflow-providers-standard` (можно ещё на 2.x).
5. **Шаг 5** — Проверить кастомные операторы/таски на **прямой доступ к метабазе** — в 3.x запрещён.
6. **Шаг 6** — Обновить инстанс: `airflow config update --fix` + `airflow db migrate` + новый образ 3.2.1.
7. **Шаг 7** — Переписать скрипты запуска: `webserver` → `api-server`, добавить `dag-processor`.
8. **Шаг 8** — Постпроверки: SSO/FAB, удалённые фичи, новые дефолты.

---

## 1. Архитектурные изменения Airflow 3.x

### 1.1. Концептуально

| Компонент | Airflow 2.x | Airflow 3.x |
|-----------|-------------|-------------|
| Доступ к метабазе | Воркеры/таски напрямую через SQLAlchemy | **Запрещён.** Только через Task Execution API |
| Web UI | Flask-сервер | Новый UI на React + **API server** (FastAPI) |
| DAG processing | Внутри scheduler'а | **Отдельный сервис** `dag-processor` (обязательный) |
| Triggerer | Опционально | Фактически обязателен (deferrable стало стандартом) |
| REST API | `/api/v1` (stable v1) | `/api/v2` (FastAPI). v1 **удалён** |
| Auth | `[api] auth_backends` | `[core] auth_manager` (по умолчанию `SimpleAuthManager`) |
| DAG SDK | `from airflow import DAG`, операторы из `airflow.*` | `from airflow.sdk import DAG, task, Asset, ...` |
| Datasets | `Dataset` | Переименовано в **`Asset`** (старое имя — алиас, deprecated) |
| Расписание | `schedule_interval=` | Только `schedule=` |
| SubDAG | `SubDagOperator` | **Удалён**, использовать TaskGroup + Assets |
| SLA | `sla=`, SLA misses | **Удалён**, заменён на Deadline Alerts |
| Sequential/`*KubernetesExecutor` | Поддерживались | **Удалены**, использовать LocalExecutor + multi-executor |
| Минимум Python | 3.8 | **3.10+** (рекомендуется 3.12) |
| Минимум Postgres | 12+ | **13+** |

### 1.2. Ключевое архитектурное правило 3.x

> Task-код больше **не имеет** прямого доступа к Airflow metadata DB.
> Все runtime-операции (XCom, state, Variables, Connections) идут
> через **Task Execution API**. Это означает изоляцию воркеров от БД
> и совместимый интерфейс через **Task SDK**.

---

## 2. Шаг 1 — Предварительные условия

Проверочный чек-лист:

- [x] Текущая версия Airflow = **2.7.3** (≥ 2.7 — ОК, прямой апгрейд допустим).
- [ ] Python в стандартном образе `apache/airflow:2.7.3-python3.11` — **3.11**.
      В 3.2.1 минимум 3.10, рекомендуется 3.12 → планируем смену базы на `python3.12`.
- [ ] Проверить, что в DAG'ах **не используются** удалённые фичи (см. §9 «Breaking changes»):
      `SubDagOperator`, `SequentialExecutor`, `SLA`, `--subdir/-S`, `/api/v1`,
      удалённые `tomorrow_ds`/`yesterday_ds`/`prev_execution_date` и т.п.

```bash
# Быстрая проверка: что у нас сейчас стоит
docker compose exec airflow-webserver airflow version
docker compose exec airflow-webserver python --version
```

---

## 3. Шаг 2 — Очистка и резервное копирование

### 3.1. Очистка ненужных метаданных

Долго работающий Airflow накапливает XCom-данные, логи task instance,
старые DAG runs. Это удлинит schema-миграцию. Чистим **до** апгрейда:

```bash
# Удалить записи старше 30 дней (XCom, TaskInstance, DagRun, JobRun)
docker compose exec airflow-scheduler-1 \
    airflow db clean --clean-before-timestamp \
    "$(date -u -d '30 days ago' +%Y-%m-%dT%H:%M:%S)+00:00" --yes
```

### 3.2. Проверка отсутствия ошибок парсинга DAG'ов

```bash
docker compose exec airflow-scheduler-1 airflow dags reserialize
# должно отработать без AirflowDagDuplicatedIdException и прочих ошибок
```

### 3.3. Бэкап (обязательно)

```bash
mkdir -p backups

# 1) Метабаза Postgres (полный custom-format дамп)
docker compose exec postgres pg_dump -U airflow -d airflow \
    --format=custom --file=/tmp/airflow_2_7_3.dump
docker cp airflow_postgres:/tmp/airflow_2_7_3.dump ./backups/

# 2) Конфиги и DAG'и
tar czf backups/airflow_repo_$(date +%F).tgz dags plugins config .env

# 3) Снимок установленных пакетов (для отката или сравнения)
docker compose exec airflow-scheduler-1 pip freeze \
    | tee backups/pip_freeze_$(date +%F).txt
```

> **Если нет «горячего» бэкапа** — остановите Airflow перед `pg_dump`,
> иначе резерв не будет содержать всех TaskInstances/DagRuns.

---

## 4. Шаг 3 — Проверка DAG'ов с помощью `ruff` (AIR-правила)

Apache поставляет проверочную утилиту через `ruff` с правилами:

| Правило | Значение |
|---------|----------|
| `AIR301` | **Breaking** в 3.x — обязательно править |
| `AIR302` | **Breaking** в 3.x — обязательно править |
| `AIR311` | Не-breaking, но настоятельно рекомендуется обновить |
| `AIR312` | Не-breaking, но настоятельно рекомендуется обновить |

### 4.1. Установка и прогон

```bash
# Локально (минимум 0.13.1; AIR3xx-правила работают и без установленного Airflow)
pip install "ruff>=0.13.1"

# Проверить
ruff check dags/ --select AIR301,AIR302,AIR311,AIR312

# Посмотреть, что будет исправлено
ruff check dags/ --select AIR301 --show-fixes

# Применить безопасные автофиксы
ruff check dags/ --select AIR301,AIR302 --fix

!!! Шаги ниже под вопросом, обязательно проработать если включать в инструкцию
# Применить ещё и небезопасные (меняют пути импортов)
ruff check dags/ --select AIR301,AIR302 --fix --unsafe-fixes

# F401 — удалить «осиротевшие» импорты после фиксов
ruff check dags/ --select F401 --fix
```

### 4.2. Ключевые изменения импортов (`airflow.sdk`)

| Старый импорт (deprecated) | Новый импорт |
|----------------------------|--------------|
| `airflow.decorators.dag` | `airflow.sdk.dag` |
| `airflow.decorators.task` | `airflow.sdk.task` |
| `airflow.decorators.task_group` | `airflow.sdk.task_group` |
| `airflow.decorators.setup` / `teardown` | `airflow.sdk.setup` / `teardown` |
| `airflow.models.dag.DAG` | `airflow.sdk.DAG` |
| `airflow.models.baseoperator.BaseOperator` | `airflow.sdk.BaseOperator` |
| `airflow.models.param.Param`, `ParamsDict` | `airflow.sdk.Param`, `ParamsDict` |
| `airflow.models.baseoperatorlink.BaseOperatorLink` | `airflow.sdk.BaseOperatorLink` |
| `airflow.sensors.base.BaseSensorOperator` | `airflow.sdk.BaseSensorOperator` |
| `airflow.hooks.base.BaseHook` | `airflow.sdk.BaseHook` |
| `airflow.notifications.basenotifier.BaseNotifier` | `airflow.sdk.BaseNotifier` |
| `airflow.utils.task_group.TaskGroup` | `airflow.sdk.TaskGroup` |
| `airflow.utils.context.Context` | `airflow.sdk.Context` |
| `airflow.datasets.Dataset` | `airflow.sdk.Asset` |
| `airflow.datasets.DatasetAlias`/`All`/`Any` | `airflow.sdk.AssetAlias`/`All`/`Any` |
| `airflow.models.connection.Connection` | `airflow.sdk.Connection` |
| `airflow.models.variable.Variable` | `airflow.sdk.Variable` |
| `airflow.io.*` | `airflow.sdk.io.*` |

**Хронология:** в 3.1 старые импорты ещё работают (DeprecationWarning),
в будущих версиях будут удалены.

### 4.3. Семантические правки DAG'ов

| Что | Было | Стало |
|-----|------|-------|
| Расписание | `schedule_interval="@hourly"` | `schedule="@hourly"` |
| Datasets | `Dataset("s3://...")` | `Asset("s3://...")` |
| SubDAG | `SubDagOperator(...)` | `with TaskGroup("g") as tg: ...` |
| Контекст | `{{ execution_date }}` | `{{ logical_date }}` |
| SLA | `PythonOperator(..., sla=timedelta(...))` | `@deadline(timedelta(...))` (`airflow.sdk`) |
| TaskFlow | `from airflow.decorators import task` | `from airflow.sdk import task` |
| `provide_context=True` | Передавался явно | Не нужен — контекст всегда доступен |

---

## 5. Шаг 4 — Установить `apache-airflow-providers-standard`

В 3.x часть «корневых» операторов вынесена в отдельный провайдер:
`BashOperator`, `PythonOperator`, `EmptyOperator`, `ExternalTaskSensor`,
`FileSensor` и др. → теперь в `apache-airflow-providers-standard`.

> Провайдер **можно поставить заранее, ещё на 2.x**.
> Тогда DAG'и сразу переписываются на новые импорты, и в момент
> переключения на 3.2.1 переход проходит чище.

```bash
# Установить заранее в текущий 2.7.3 (опционально, рекомендуется)
docker compose exec airflow-scheduler-1 \
    pip install "apache-airflow-providers-standard"

#Так не получилось, делал через
# 1. Дописать в requirements.txt одну строку:
# echo "apache-airflow-providers-standard" >> requirements.txt

# # 2. Пересобрать образ и поднять стек
# docker compose build
# docker compose up -d
# В DAG'ах заменить
# from airflow.operators.bash import BashOperator
# →
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
```

---

## 6. Шаг 5 — Проверка кастомных задач на прямой доступ к метабазе

В Airflow 3 **task-код не может** напрямую открывать SQLAlchemy-сессию
к метабазе. Аудит — обязательная часть миграции.

### 6.1. Что искать в коде

```bash
# Грубый, но эффективный поиск «опасных» паттернов
grep -rnE 'create_session|Session\(\)|from airflow.utils.db|@provide_session|settings\.engine' dags/ plugins/
grep -rnE 'from airflow\.models import' dags/ plugins/
```

Подозрительные паттерны:

- `from airflow.utils.db import create_session`
- `@provide_session` / `session=NEW_SESSION`
- Импорт моделей: `from airflow.models import DagRun, TaskInstance, XCom`
- `settings.engine`, `settings.Session`

### 6.2. Рекомендуемое решение — Airflow Python Client

Использовать официальный REST-клиент через API server:

```bash
pip install apache-airflow-client
```

```python
import airflow_client.client as ac
cfg = ac.Configuration(host="http://airflow-api-server:8080/api/v2")
# токен получается через POST /auth/token
```

**Плюсы:** изоляция, токен-аутентификация, нет драйверов БД на воркерах.
**Минусы:** не все операции покрыты API.

### 6.3. Обходное решение — `DbApiHook` (не рекомендуется)

> ⚠️ Перестанет работать в Airflow ≥ 3.2 при изменении схемы.
> Используйте **только** если кейс не покрывается Python-клиентом.

```python
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task
def get_connections():
    hook = PostgresHook(postgres_conn_id="metadata_postgres")
    return hook.get_records(
        "SELECT conn_id, conn_type FROM connection LIMIT 10"
    )
```

Завести Airflow-Connection `metadata_postgres`, указывающий на нашу
метабазу. Это будет прямое подключение через `psycopg2`, **в обход** API.

---

## 7. Шаг 6 — Обновление инстанса (config + db + образы)

### 7.1. Утилита `airflow config update`

```bash
# Сухой прогон — покажет, что в текущем airflow.cfg/env устарело
docker compose exec airflow-scheduler-1 airflow config update
# ПОхоже можно использовать только на 3.0+ такие команды
# Автоматически починить совместимые ключи
docker compose exec airflow-scheduler-1 airflow config update --fix
```
docker compose exec airflow db migrate
использовать тоже после 
> Запускать пока стенд ещё на 2.7.3 — утилита поднимет deprecated-ключи,
> которые ломаются в 3.x.

### 7.2. Адаптация `Dockerfile`

```dockerfile
FROM apache/airflow:3.2.1-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Версии всех провайдеров автоматически берутся из официального constraints-файла.
RUN pip install --no-cache-dir \
    "apache-airflow[celery,postgres,fab]==3.2.1" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.12.txt"

# requirements.txt — только дополнительные библиотеки поверх extras (если есть)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.12.txt"
```

### 7.3. Адаптация `docker-compose.yml`

Сервисы в 3.x:

| Сервис | Команда | Заметки |
|--------|---------|---------|
| `airflow-api-server` | `api-server` | **заменяет** `webserver`, порт 8080 |
| `airflow-scheduler-1/2` | `scheduler` | без изменений в количестве |
| `airflow-dag-processor` | `dag-processor` | **новый, обязательный** |
| `airflow-triggerer` | `triggerer` | становится обязательным |
| `airflow-worker-1/2` | `celery worker` | без изменений в количестве |
| `airflow-init` | `db migrate` + создание admin | без изменений |

#### Устаревшие конфигурационные параметры

При переходе на 3.2.1 часть параметров `airflow.cfg` (и соответствующих
env-переменных `AIRFLOW__SECTION__KEY`) **удалена, переименована или
перенесена** в другую секцию или провайдер. Такие ключи нужно либо убрать
из `docker-compose.yml`/`airflow.cfg`, либо переименовать.

| Было (2.7.3, `секция/ключ`) | Стало (3.2.1) | Тип изменения |
|------------------------------|---------------|----------------|
| `[webserver] secret_key` | `[api] secret_key` | переименовано |
| `[webserver] expose_config` | `[api] expose_config` | переименовано |
| `[webserver] base_url` | `[api] base_url` | переименовано |
| `[webserver] *` (большинство ключей) | `[api]` + `[fab]` | секция расщеплена |
| `[api] auth_backends` | `[core] auth_manager` | концептуальная замена (один менеджер вместо списка backend'ов) |
| `[core] sql_alchemy_conn` | `[database] sql_alchemy_conn` | перенесено (в 2.7 уже работало, в 3.x старый ключ удалён) |
| `[core] dag_concurrency` | `[core] max_active_tasks_per_dag` | переименовано |
| `[scheduler] dag_dir_list_interval` | `[dag_processor] refresh_interval` | перенесено в новую секцию |
| `[scheduler] standalone_dag_processor` | `[dag_processor] enabled` | перенесено, в 3.x включено по умолчанию |
| `[core] check_slas` | — | удалено (механизм SLA снят, заменён Deadline Alerts) |
| `[core] dag_pickle`, `[core] do_pickle` | — | удалено (pickling DAG'ов снят) |
| `[core] hide_sensitive_var_conn_fields` | `[core] sensitive_var_conn_names` | переименовано / семантика изменена |

На уровне нашего `docker-compose.yml` это означает такие переименования
env-переменных:

```text
AIRFLOW__WEBSERVER__SECRET_KEY              →  AIRFLOW__API__SECRET_KEY
AIRFLOW__WEBSERVER__EXPOSE_CONFIG           →  AIRFLOW__API__EXPOSE_CONFIG
AIRFLOW__WEBSERVER__BASE_URL                →  AIRFLOW__API__BASE_URL
AIRFLOW__API__AUTH_BACKENDS                 →  удалить; перейти на [core] auth_manager
AIRFLOW__CORE__SQL_ALCHEMY_CONN             →  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
AIRFLOW__CORE__DAG_CONCURRENCY              →  AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG
AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL   →  AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL
```
!ВАЖНО!
Ошибка 1
В Airflow 3.x появился отдельный механизм аутентификации между компонентами (scheduler/worker/triggerer ↔ api-server) через JWT — это часть новой Task Execution API (одно из главных архитектурных изменений 3.x: воркеры больше не ходят в БД напрямую, а только через api-server, и каждый запрос подписан JWT).
На стадии запуска возникла проблема с JWT токеном. Лечил добавлением в .cfg переменной jwt_secret.
генерил с помощью команды python3 -c "import secrets; print(secrets.token_urlsafe(64))"

Ошибка 2
httpx.ConnectError: [Errno 111] Connection refused у воркера
В Airflow 3.x воркеры больше не ходят в БД напрямую — каждое обновление состояния таски они шлют HTTP-запросом в api-server (это и есть Task Execution API). Адрес api-server задаётся отдельной настройкой:
execution_api_server_url=http://airflow-api-server:8080/execution/ # указываем днс апи сервера 

**Как обнаружить deprecated-параметры в текущем стенде (2.7.3)**

- Захватить `DeprecationWarning` из stderr — Airflow логирует их при чтении устаревших ключей:
  ```bash
  docker compose exec airflow-scheduler-1 sh -c \
      'airflow config list 2>&1' | grep -iE 'deprecat|removed'
  ```
- Узнать источник конкретного значения (env / cfg / default):
  ```bash
  docker compose exec airflow-scheduler-1 \
      airflow config get-value --include-source core executor
  ```
- Сравнить cfg между версиями образа (без поднятия стека):
  ```bash
  docker run --rm apache/airflow:2.7.3-python3.11 cat /opt/airflow/airflow.cfg > /tmp/2.7.3.cfg
  docker run --rm apache/airflow:3.2.1-python3.12 cat /opt/airflow/airflow.cfg > /tmp/3.2.1.cfg
  diff -u /tmp/2.7.3.cfg /tmp/3.2.1.cfg | tee backups/cfg_diff.txt
  ```
- Проверить логи на стартовые DeprecationWarning:
  ```bash
  docker compose logs airflow-scheduler-1 | grep -i 'deprecat'
  ```
- **После** установки образа 3.2.1 — авторитетный источник: `airflow config update` (dry-run) и `airflow config update --fix` (автофикс).

### 7.4. Миграция БД

```bash
# 1. Остановить компоненты, оставить Postgres
docker compose stop airflow-webserver airflow-scheduler-1 airflow-scheduler-2 \
                    airflow-worker-1 airflow-worker-2 airflow-triggerer

# 2. Собрать новый образ 3.2.1
docker compose build

# 3. Запустить миграцию схемы
docker compose run --rm airflow-init airflow db migrate

# 4. Поднять полный стек
docker compose up -d
```

### 7.5. Плагины Flask-AppBuilder

Если есть плагины с `appbuilder_views`, `appbuilder_menu_items`,
`flask_blueprints` — два варианта:

1. **Установить FAB-провайдер** (обратная совместимость) — быстрое решение.
2. **Перевести на Airflow 3 plugin interface**: `external_views`,
   `fastapi_apps`, `fastapi_root_middlewares` — рекомендуется.

### 7.6. Helm Chart (если применимо)

В нашем стенде Helm не используется, но для справки:
все секции `webserver.*` в `values.yaml` переименовать на `apiServer.*`.
Много ключей переименовано/удалено.

---

## 8. Шаг 7 — Изменения в скриптах запуска

### 8.1. Новые команды CLI

```bash
# Было (Airflow 2.x)
airflow webserver

# Стало (Airflow 3.x)
airflow api-server          # FastAPI-сервер, заменяет webserver
airflow dag-processor       # ОБЯЗАТЕЛЬНО запускать отдельно, даже локально
airflow scheduler           # без изменений
airflow triggerer           # становится фактически обязательным
airflow celery worker       # без изменений
```

### 8.2. Создание admin-пользователя

Команда `airflow users create` теперь живёт в `apache-airflow-providers-fab`:

```bash
docker compose exec airflow-api-server \
    airflow users create \
        --username admin --password admin \
        --firstname Admin --lastname Admin \
        --role Admin --email admin@example.com
```

Если используется `SimpleAuthManager` (без FAB) — пользователи задаются
в `[simple_auth_manager] users` конфига, см. документацию.

---

## 9. Шаг 8 — Постпроверки и breaking changes

### 9.1. После обновления проверить

- **SSO/OAuth/OIDC/LDAP** — авторизация работает.
  Если есть кастомный `webserver_config.py` с
  `from airflow.www.security import AirflowSecurityManager` — заменить на:
  ```python
  from airflow.providers.fab.auth_manager.security_manager.override \
      import FabAirflowSecurityManagerOverride
  ```
- URL `oauth-authorized/<provider>` теперь под префиксом `/auth/`:
  `https://<host>/auth/oauth-authorized/google` (было без `/auth`).
- Healthcheck'и зелёные на всех сервисах.

### 9.2. Удалённые фичи (если используются — ломается)

| Фича | Замена |
|------|--------|
| `SubDagOperator` | `TaskGroup` + Assets + Data-Aware Scheduling |
| `SequentialExecutor` | `LocalExecutor` (работает с SQLite для разработки) |
| `CeleryKubernetesExecutor`, `LocalKubernetesExecutor` | Multi-executor конфигурация |
| `SLA` (`sla=`, SLA misses) | **Deadline Alerts** |
| `--subdir` / `-S` в CLI | **DAG bundles** |
| REST API `/api/v1` | `/api/v2` (FastAPI) |
| `auth_backends` | `[core] auth_manager` |
| `[webserver]` секция конфига | `[api]` + `[fab]` |
| `airflow.www.security.AirflowSecurityManager` | `FabAirflowSecurityManagerOverride` (см. §9.1) |

### 9.3. Удалённые context-переменные в task instance

Использование этих ключей **сломает DAG**:

```text
tomorrow_ds          tomorrow_ds_nodash
yesterday_ds         yesterday_ds_nodash
prev_ds              prev_ds_nodash
prev_execution_date  prev_execution_date_success
next_execution_date  next_ds                next_ds_nodash
execution_date  →   logical_date
```

### 9.4. Новые дефолты (могут поменять поведение)

| Параметр | Новый дефолт | Эффект |
|----------|--------------|--------|
| `catchup_by_default` | `False` | DAG'и больше не «догоняют» историю автоматически — задаём явно `catchup=True`, если нужно |
| `create_cron_data_intervals` | `False` | Используется `CronTriggerTimetable` вместо `CronDataIntervalTimetable` |
| `[core] auth_manager` | `SimpleAuthManager` | Для FAB-UI нужно явно ставить FAB-провайдер и менять `auth_manager` |

---

## 10. Адаптация под наш стенд (сводка)

### 10.1. Что меняется в файлах репозитория

| Файл | Изменение |
|------|-----------|
| `Dockerfile` | `FROM apache/airflow:3.2.1-python3.12`, ставим airflow с extras+constraints |
| `requirements.txt` | Очистить — версии провайдеров идут из extras+constraints |
| `docker-compose.yml` | `webserver` → `api-server`, добавить `dag-processor` и `triggerer`, удалить deprecated env-vars |
| `.env` | Добавить `AIRFLOW_API_SECRET_KEY`, удалить `AIRFLOW_WEBSERVER_SECRET_KEY` |
| `dags/*` | Прогнать `ruff --fix`, заменить импорты на `airflow.sdk.*`, `schedule_interval=` → `schedule=` и т.д. |
| `plugins/*` | Аудит на прямой доступ к БД (см. §6) |

### 10.2. Целевые версии провайдеров

При `pip install "apache-airflow[celery,postgres,fab]==3.2.1" --constraint ...`
автоматически встанут (срез по constraints-3.2.1/3.12):

| Провайдер | Сейчас (2.7.3) | После (3.2.1) |
|-----------|----------------|---------------|
| `apache-airflow-providers-celery` | 3.4.x | **3.10+** |
| `apache-airflow-providers-postgres` | 5.7.x | **6.0+** |
| `apache-airflow-providers-fab` | — (в составе ядра) | **2.0+** |
| `apache-airflow-providers-standard` | — | **1.0+** |

---

## 11. План отката

Если после миграции стенд нестабилен:

```bash
# 1) Остановить новый стек
docker compose down

# 2) Откатить инфраструктурные файлы к baseline
git checkout v2.7.3-baseline -- Dockerfile docker-compose.yml requirements.txt .env

# 3) Восстановить метабазу из дампа
docker compose up -d postgres
docker cp ./backups/airflow_2_7_3.dump airflow_postgres:/tmp/
docker compose exec postgres dropdb -U airflow airflow
docker compose exec postgres createdb -U airflow airflow
docker compose exec postgres pg_restore -U airflow -d airflow /tmp/airflow_2_7_3.dump

# 4) Поднять старый стек
docker compose up -d
```

> Перед апгрейдом обязательно поставить git-тег `v2.7.3-baseline`.

---

## 12. Чек-лист миграции

**Шаг 1 — Предусловия**
- [ ] Airflow ≥ 2.7 (у нас 2.7.3 — ✅)
- [ ] Python в новом образе ≥ 3.10
- [ ] Подтверждено отсутствие удалённых фич (см. §9.2)

**Шаг 2 — Бэкап**
- [ ] `airflow db clean` выполнен
- [ ] `pg_dump` метабазы → `backups/airflow_2_7_3.dump`
- [ ] `tar` каталогов `dags/`, `plugins/`, `config/`, `.env`
- [ ] `pip freeze` сохранён

**Шаг 3 — DAG'и**
- [ ] `ruff check --select AIR301,AIR302` чисто
- [ ] `ruff check --select AIR311,AIR312` чисто или согласовано
- [ ] Импорты переведены на `airflow.sdk.*`
- [ ] `schedule_interval=` → `schedule=` везде
- [ ] `Dataset` → `Asset`, `SubDagOperator` → `TaskGroup`, `sla=` убран

**Шаг 4 — Standard provider**
- [ ] `apache-airflow-providers-standard` установлен
- [ ] Импорты Bash/Python/Empty переведены на `airflow.providers.standard.*`

**Шаг 5 — Доступ к БД**
- [ ] Кастомные таски проверены на прямой доступ к метабазе
- [ ] Где надо — переход на `apache-airflow-client` или `DbApiHook`

**Шаг 6 — Апгрейд**
- [ ] `airflow config update --fix` выполнен
- [ ] `Dockerfile` на `apache/airflow:3.2.1-python3.12`
- [ ] `docker-compose.yml`: `api-server`, `dag-processor`, `triggerer`
- [ ] `airflow db migrate` отработал без ошибок
- [ ] Удалены `AIRFLOW__WEBSERVER__*`

**Шаг 7 — Запуск**
- [ ] Новые команды: `api-server`, `dag-processor`
- [ ] Admin-пользователь создан

**Шаг 8 — Постпроверки**
- [ ] SSO/OAuth/LDAP работают
- [ ] Healthcheck'и зелёные у всех сервисов
- [ ] Тестовый DAG `example_celery_smoke` end-to-end зелёный
- [ ] `catchup=` задан явно в нужных DAG'ах
- [ ] План отката отработан на staging

---

## 13. Полезные ссылки

* Официальное руководство по апгрейду: <https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html>
* Release notes 3.0: <https://airflow.apache.org/docs/apache-airflow/3.0.0/release_notes.html>
* Release notes 3.2: <https://airflow.apache.org/docs/apache-airflow/3.2.1/release_notes.html>
* Datasets → Assets: <https://airflow.apache.org/docs/apache-airflow/3.2.1/authoring-and-scheduling/assets.html>
* Task SDK: <https://airflow.apache.org/docs/apache-airflow-task-sdk/>
* Constraints для 3.2.1 / Python 3.12: <https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.12.txt>
* Airflow Python Client: <https://airflow.apache.org/docs/apache-airflow-python-client/>
* Ruff AIR rules: <https://docs.astral.sh/ruff/rules/#airflow-air>

> Версии в этом документе зафиксированы на момент составления плана.
> Перед фактическим выполнением миграции **обязательно** свериться с
> актуальными release notes Airflow 3.2.x.


Этап B — Финальный снимок + останов 2.7.3
bash
cd /home/eduard/work/t1/upgrade_airflow_to_3x

# 1. Снимок (~2 мин)
SNAP="backups/cutover_$(date +%F_%H%M)"
mkdir -p "$SNAP"
docker compose exec -T postgres \
    pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists \
    > "$SNAP/airflow_db.sql"
docker compose exec airflow-scheduler-1 airflow config list  > "$SNAP/cfg_2.7.3.txt" 2>&1
docker compose exec airflow-scheduler-1 airflow version       > "$SNAP/version.txt"
docker compose exec airflow-scheduler-1 airflow providers list > "$SNAP/providers.txt"
docker compose exec airflow-scheduler-1 pip freeze            > "$SNAP/pip_freeze.txt"

# 2. Останов БЕЗ -v (тома сохраняем!)
docker compose stop

Этап D — Сборка образа 3.2.1 (5–10 мин)
bash
docker compose build --no-cache
docker images | grep airflow-custom   # должен появиться airflow-custom:3.2.1


Этап E — Конфиг + миграция БД на новом образе (3–5 мин)
bash
# Сухой прогон утилиты config update — посмотрим, что нужно поправить
docker compose run --rm airflow-scheduler-1 airflow config update

# Удалить orphan-контейнер старого webserver (тома НЕ трогаются)
docker rm airflow_webserver
Или сразу при следующем up:
bash
docker compose up -d --remove-orphans

# Применить автофикс (правит наш airflow.cfg)
docker compose run --rm airflow-scheduler-1 airflow config update --fix

# Накатить миграции схемы метаданных
docker compose run --rm airflow-scheduler-1 airflow db migrate

# Проверка
docker compose run --rm airflow-scheduler-1 airflow db check-migrations
docker compose run --rm airflow-scheduler-1 airflow ver

Этап F — Старт 3.2.1
bash
# Удалить orphan-контейнер старого webserver (тома НЕ трогаются)
docker rm airflow_webserver
Или сразу при следующем up:
bash
docker compose up -d --remove-orphans

Этап G — Smoke-тест
bash
curl -fsS http://localhost:8080/api/v2/version | jq
docker compose exec airflow-scheduler-1 airflow dags list-import-errors  # пусто = норм
docker compose exec airflow-scheduler-1 airflow dags list
docker compose exec airflow-api-server   airflow jobs check --job-type SchedulerJob
docker compose exec airflow-dag-processor airflow jobs check --job-type DagProcessorJob
docker compose exec airflow-triggerer    airflow jobs check --job-type TriggererJob

Артефакты после dry-run (касательно cfg)

🔴 BREAKING — удалён	[logging] log_filename_template	—	Удалить из cfg. В 3.x шаблон имени файла лога зашит в код (task_id={ti.task_id}/run_id={ti.run_id}/...). Кастомизировать его теперь нельзя через cfg — только через provider'ы.
🟡 BREAKING — переименован	[webserver] cookie_samesite	[fab] cookie_samesite	Перенести в секцию [fab]. Семантика та же — настройка SameSite-куки сессии. Теперь принадлежит FAB-провайдеру (он отвечает за UI + аутентификацию).
🟡 BREAKING — переименован	[webserver] cookie_secure	[fab] cookie_secure	То же — перенести в [fab]. Управляет флагом Secure для cookie сессии (только по HTTPS).
