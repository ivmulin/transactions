# Real-Time Crypto Data Pipeline

End-to-end data pipeline для сбора и обработки данных о криптовалютном рынке в реальном времени.

Проект получает тиковые данные с Binance, передаёт события через Apache Kafka, сохраняет их в PostgreSQL, преобразует с помощью dbt (staging → incremental marts, покрытые автоматическими тестами) и оркестрирует расчёт витрин с помощью Apache Airflow (task-per-model, через `astronomer-cosmos`).

Проект — учебный: цель заключалась в освоении production-паттернов data engineering, а не в первую очередь в завершении фичи. Ниже — не только архитектура, но и реальные инциденты, с которыми пришлось разбираться по пути, и как они были решены.

## Статус проекта

**Проект закрыт на достигнутой границе — не заброшен, а осознанно остановлен.**

Полный end-to-end контур (Kafka → Postgres → dbt incremental → Airflow-шедулинг → dbt-тесты) был развёрнут и подтверждённо работал: после включения 5-минутного расписания в Airflow несколько циклов подряд отработали успешно, freshness-тест переходил из FAIL в PASS ровно так, как ожидалось. Затем регулярный запуск `dbt run`/`dbt test` поверх уже работающего стека (Kafka + 2×Postgres + Airflow) исчерпал память VPS (1.9GB RAM) — процессы были убиты OOM killer'ом, что подтверждено напрямую через VNC-консоль хостинга.

Осознанное решение: регулярный Airflow-шедулинг остановлен (`docker compose stop airflow-scheduler airflow-webserver`). Это не технический долг — это выбор не апгрейдить железо ради пет-проекта. Инфраструктура и DAG остаются в репозитории рабочим, проверенным артефактом и могут быть подняты снова в любой момент.

## Архитектура

```text
Binance API
     │
     ▼
┌─────────────────┐
│    Producer      │  Python / asyncio / aiohttp
└────────┬─────────┘
         ▼
┌─────────────────┐
│      Kafka        │  KRaft mode
└────────┬─────────┘
         ▼
┌─────────────────┐
│    Consumer       │  Python / aiokafka, batch insert, manual offset commit
└────────┬─────────┘
         ▼
┌─────────────────┐
│  PostgreSQL DWH   │  binance_raw_prices
└────────┬─────────┘
         ▼
┌─────────────────┐
│   dbt staging      │  stg_binance__raw_prices (view, source() → renamed)
└────────┬─────────┘
         ▼
┌─────────────────┐
│    dbt marts        │  fct_candles_1m (incremental, merge, unique_key=[symbol,bucket])
└────────┬─────────┘
         ▼
┌─────────────────┐
│   dbt tests          │  freshness / OHLC-инвариант / gap detection
└─────────────────┘

┌───────────────────────────────┐
│   Airflow + astronomer-cosmos   │  task-per-model DAG (по умолчанию остановлен)
└───────────────────────────────┘
```

### Архитектурное решение: единственный источник правды по свечам

На раннем этапе 1-минутные OHLCV-свечи считались отдельным Python-сервисом (фоновый цикл, `FIRST_VALUE`/`LAST_VALUE` + `ON CONFLICT` upsert в таблицу `binance_candles_1m`). После того как dbt-модель `fct_candles_1m` покрыла ту же задачу декларативно и с тестами, было принято архитектурное решение: **dbt становится единственным источником правды**. Python-трансформер выведен из эксплуатации, таблица `binance_candles_1m` дропнута — держать одну и ту же логику агрегации в двух параллельных, независимо дрейфующих реализациях означало два разных источника правды без единого владельца.

## Data flow

```text
Binance API → Producer → Kafka → Consumer → PostgreSQL DWH
                                                    │
                                                    ▼
                                        dbt staging (source → ref)
                                                    │
                                                    ▼
                                    dbt marts (incremental, tested)
                                                    │
                                                    ▼
                                       Airflow (task-per-model оркестрация)
```

## Основные инженерные решения

### Асинхронный сбор данных
Producer использует `asyncio`, `aiohttp` и `aiokafka` для асинхронного получения тиков с Binance и публикации в Kafka без блокировки на сетевом I/O.

### Kafka как промежуточный streaming layer
Producer и Consumer работают независимо, Kafka — буфер между внешним API и PostgreSQL. Это позволяет масштабировать и переживать сбои каждого компонента отдельно.

### Батчевая запись и manual offset commit
Consumer накапливает сообщения в батчи (по размеру или таймауту) и коммитит offset в Kafka только после успешной записи батча в PostgreSQL — при ошибке записи offset не подтверждается, предусмотрен retry. Таким образом Consumer никогда не подтверждает обработку сообщения раньше, чем данные реально сохранены.

### dbt: staging → incremental marts → тесты
Модели разделены по слоям с чёткой границей: `source()` используется только в staging, `ref()` — везде дальше, что делает граф зависимостей dbt детерминированным и видимым через `dbt docs generate`.

`fct_candles_1m` — incremental-модель (`merge`-стратегия, `unique_key=['symbol', 'bucket']`), с overlap-окном через `{{ this }}` вместо абсолютного `NOW()` — это гарантирует, что модель самостоятельно досчитывает пропущенный период после простоя любой длины, а не теряет данные при перерыве в шедулинге.

Три параметризованных generic-теста написаны с нуля:
* **freshness** — лаг `MAX(bucket)` от текущего момента по группам `symbol`, настраиваемые WARNING/CRITICAL пороги
* **ohlc_candle** — геометрический инвариант свечи (high ≥ open/close/low, low ≤ open/close, цены и `ticks_count` строго положительны)
* **missing_candles** — gap detection через генерацию временной сетки, декартово произведение с тикерами и anti-join на фактические данные

### Оркестрация: Airflow + astronomer-cosmos, task-per-model
Вместо одной bash-команды (`dbt run && dbt test` внутри `BashOperator`) DAG использует `astronomer-cosmos`, который транслирует граф зависимостей dbt-проекта в отдельные Airflow-таски — по одному на модель и на тест. Это даёт точечную видимость провала (в UI видно, какая именно модель/тест упали, а не один общий "красный" таск) и точечный ретрай без пересчёта уже успешно отработавших моделей.

DAG (`airflow/dags/dbt_crypto_dag.py`) построен через `DbtDag` с тремя конфиг-объектами Cosmos:
* `ProjectConfig` — путь к dbt-проекту внутри контейнера
* `ProfileConfig` — отдельный `profiles.airflow.yml`, target `airflow_container`, подключение к DWH напрямую по имени Docker-сервиса, без SSH-туннеля
* `ExecutionConfig` — путь к изолированному dbt-бинарнику (`/opt/airflow/dbt_venv/bin/dbt`)

Первый ручной `trigger` дал 9/10 тестов PASS, один ожидаемый FAIL (freshness — до первого прогона по расписанию таблица объективно устарела). После включения 5-минутного расписания freshness-тест подтверждённо переходил в PASS — end-to-end контур был технически подтверждён рабочим до финального инцидента с нехваткой памяти.

### Контейнеризация
Все компоненты — в Docker, окружение описано через Docker Compose: PostgreSQL DWH, Kafka, Python producer/consumer, Airflow (webserver + scheduler + отдельная metadata-БД, изолированная от DWH). dbt для Airflow ставится в изолированный venv отдельным `Dockerfile.airflow`, чтобы не конфликтовать с зависимостями самого Airflow-образа.

## Инциденты и их решения

Ниже — реальные проблемы, с которыми пришлось разбираться по ходу проекта, не сглаженные до "всё работало сразу".

1. **`docker compose down <service>` роняет весь проект, а не один сервис.** Попытка убрать только `transformer` неожиданно остановила и producer, и consumer — `down` работает на уровне всего Compose-проекта, а не отдельного сервиса. Решение: точечное удаление через `docker compose stop <service>` + `docker compose rm -f <service>`.

2. **UID mismatch на смонтированных volume.** Процессы внутри образа Airflow выполняются от UID 50000 (`airflow`), а директории на хосте (`./airflow/logs`, `./dbt_crypto`) создавались от root — запись падала с `PermissionError`. Решение: `chown -R 50000:0 <path>` на хосте.

3. **pip backtracking при установке dbt в основное окружение Airflow.** Попытка поставить `dbt-core`/`dbt-postgres` вместе с Airflow приводила к затяжному разрешению зависимостей — конфликт с constraints самого Airflow-образа. Решение: изоляция dbt в отдельный venv (`/opt/airflow/dbt_venv`), Cosmos обращается к нему как к внешнему исполняемому файлу через `dbt_executable_path`, а не как к библиотеке в том же окружении.

4. **Нехватка памяти VPS при поднятии полного стека.** 1.9GB RAM не хватало на Kafka + 2×Postgres + Airflow без подкачки. Решение: 2GB swap-файл, плюс явное ограничение `KAFKA_HEAP_OPTS=-Xmx512m -Xms512m` (дефолтный JVM heap был неоправданно большим для этого объёма памяти).

5. **IPv6 connection reset при `docker pull`.** Нестабильный сетевой путь до Docker Hub с VPS-хостинга.

6. **Переменные окружения не пробрасываются между сервисами Compose автоматически.** `POSTGRES_*` нужно было явно добавить в `environment`/`env_file` каждого Airflow-сервиса отдельно — `.env` не расшаривается между сервисами сам по себе.

7. **Финальный: OOM kill при 5-минутном шедулинге dbt поверх полного стека.** Регулярные `dbt run` + `dbt test` вместе с уже работающим Kafka + 2×Postgres + Airflow исчерпали память; ядро остановило процессы через OOM killer — подтверждено напрямую через VNC-консоль хостинга, не косвенно по симптомам. Итоговое решение: регулярный шедулинг сознательно остановлен, дальнейшая оптимизация ресурсов на этом железе не проводится — апгрейд VPS ради пет-проекта признан нецелесообразным.

## Технологический стек

| Компонент         | Технология                          |
| ------------------ | ------------------------------------ |
| Язык               | Python                               |
| Асинхронный I/O    | asyncio, aiohttp                     |
| Streaming          | Apache Kafka (KRaft mode)            |
| Kafka client       | aiokafka                             |
| DWH                | PostgreSQL 15                        |
| Трансформации      | dbt-postgres (dbt 1.12.0)            |
| Оркестрация        | Apache Airflow 2.10.4 (LocalExecutor)|
| dbt ↔ Airflow      | astronomer-cosmos                    |
| Контейнеризация    | Docker, Docker Compose               |
| Контроль версий    | Git                                  |

## Структура проекта

```text
.
├── src/
│   ├── config/
│   ├── connectors/
│   ├── utils/
│   ├── main.py
│   └── consumer_main.py
│
├── dbt_crypto/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── macros/
│   │   └── tests/          # freshness, ohlc_candle, missing_candles
│   └── dbt_project.yml
│
├── airflow/
│   ├── dags/
│   │   └── dbt_crypto_dag.py
│   └── logs/
│
├── sql/
├── scripts/
├── Dockerfile
├── Dockerfile.airflow
├── docker-compose.yml
├── profiles.airflow.yml     # профиль dbt для запуска ИЗ Airflow-контейнера
└── requirements.txt
```

## Запуск

### Требования
* Docker, Docker Compose
* `.env` в корне репозитория (`POSTGRES_*`, `AIRFLOW_DB_*`, `AIRFLOW_ADMIN_*`, `AIRFLOW_IP`/`PORT`, `POSTGRES_IP`/`PORT`)

### Основной pipeline (producer → consumer → DWH)

```bash
docker compose up -d postgres kafka producer consumer
```

### Аналитический слой (dbt)

dbt запускается локально (не в контейнере) через SSH-туннель к VPS, профиль — `~/.dbt/profiles.yml`, target `dev`:

```bash
cd dbt_crypto
dbt run
dbt test
```

### Оркестрация (Airflow) — опционально

По умолчанию не запускается вместе с основным стеком — регулярный шедулинг остановлен осознанно (см. «Инциденты», п. 7). Для ручной проверки контура:

```bash
docker compose up -d airflow-postgres airflow-init airflow-webserver airflow-scheduler
```

DAG использует отдельный профиль `profiles.airflow.yml` (смонтирован в контейнер отдельно от локального `~/.dbt/profiles.yml`) и обращается к Postgres DWH напрямую по имени сервиса в общей Docker-сети, без SSH-туннеля.

Postgres и Airflow UI доступны только локально/через SSH-туннель (биндинг на конкретный IP, не на `0.0.0.0`) — сознательное ограничение доступа, а не упущенная настройка.

## Чему посвящён проект

Обучающий пет-проект: цель — практика production-паттернов data engineering, а не просто доведение фичи до конца. Освоенные темы: streaming ingestion, асинхронное программирование, батч-обработка и управление Kafka offset'ами, идемпотентная агрегация данных, dbt-моделирование (staging/marts, incremental) и параметризованные data quality тесты, оркестрация через Airflow и astronomer-cosmos, диагностика реальных инфраструктурных инцидентов (permissions, dependency conflicts, сетевые обрывы, нехватка памяти) — включая доведение до конца сложного решения (осознанная остановка) вместо бесконечной борьбы с ограничениями железа.
