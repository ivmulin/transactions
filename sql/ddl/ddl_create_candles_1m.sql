-- Создаёт витрину для хранения свечи за последнюю минуту
CREATE TABLE IF NOT EXISTS binance_candles_1m (
    ticker VARCHAR(15) NOT NULL,
    bucket TIMESTAMPTZ NOT NULL,
    open_price NUMERIC(18, 8) NOT NULL,
    high_price NUMERIC(18, 8) NOT NULL,
    low_price NUMERIC(18, 8) NOT NULL,
    close_price NUMERIC(18, 8) NOT NULL,
    ticks_count INT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, bucket)
);

-- Индекс для быстрого построения временных рядов по конкретному тикеру
CREATE INDEX IF NOT EXISTS idx_candles_1m_ticker_bucket 
ON binance_candles_1m (ticker, bucket DESC);