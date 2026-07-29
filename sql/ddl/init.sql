-- 1. Таблица для сырых тиков с Binance
CREATE TABLE IF NOT EXISTS binance_raw_prices (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    price NUMERIC(18, 8) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Композитный B-Tree индекс
CREATE INDEX IF NOT EXISTS idx_binance_prices_ticker_created 
ON binance_raw_prices (ticker, created_at DESC);