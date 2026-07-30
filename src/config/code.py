# === === === === === ===  #
# Настройки поведения кода #
# === === === === === ===  #

# === Common ===

ASYNCIO_DELAY = 1  # n sec delay
BATCH_SIZE = 5

# === DWH ===

MIN_POOL_SIZE = 1
MAX_POOL_SIZE = 10
MAX_BATCH_SIZE = 500
FLUSH_INTERVAL = 1.0
DWH_NAME = "binance_raw_prices"

# === SQL ===

SQL_FOLDER = "sql"
SQL_AGGREGATOR_1M_FOLDER = "transform"
SQL_AGGREGATOR_1M_FILE = "aggregate_1m_candles.sql"

# === ENV ===

# Names of .env variables
PSQL_DB_NAME = "POSTGRES_DB"
PSQL_USER_NAME = "POSTGRES_USER"
PSQL_PWD_NAME = "POSTGRES_PASSWORD"
PSQL_PORT_NAME = "POSTGRES_PORT"
PSQL_HOST_NAME = "POSTGRES_HOST"

# === === ===
