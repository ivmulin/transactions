-- scripts/init_db.sql

-- 1. Создание таблицы для сырых цен тикеров
CREATE TABLE IF NOT EXISTS binance_raw_prices (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    price NUMERIC(18, 8) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Композитный B-Tree индекс для оптимизации выборок по тикеру и времени
CREATE INDEX IF NOT EXISTS idx_binance_prices_ticker_created
ON binance_raw_prices (ticker, created_at DESC);
