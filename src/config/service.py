# === === === === === === === #
# Настройки поведения сервиса #
# === === === === === === === #

# === Common ===

# === KAFKA ===

import os

KAFKA_TOPIC_PRICES = "binance_prices"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_CONSUMER_GROUP = "binance_price_readers"

# === Binance ===

BINANCE_TICKERS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "NEOUSDT",
    "LTCUSDT",
    "QTUMUSDT",
    "ADAUSDT",
    "XRPUSDT",
    "TUSDUSDT",
    "IOTAUSDT",
    "XLMUSDT",
    "ONTUSDT",
    "TRXUSDT",
    "ETCUSDT",
    "ICXUSDT",
]

BINANCE_TICKER_PARAM = "symbol"

BINANCE_FETCHER_SETTINGS = {"retries": 3, "timeout": 2.0}

BINANCE_URL = "https://api.binance.com/api/v3/ticker/price"

# === Consumer ===

CONSUMER_MAX_RECORDS = 100
CONSUMER_TIMEOUT_MS = 1000

# == SQL ===

SQL_UPDATE_SLEEP = 60

# === === ===
