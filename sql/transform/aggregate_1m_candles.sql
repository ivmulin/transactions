WITH windowed_values AS (
    SELECT
        ticker,
        price,
        date_trunc('minute', created_at) AS m1_bucket,
        FIRST_VALUE(price) OVER window_1m AS open_price,
        LAST_VALUE(price) OVER window_1m AS close_price
    FROM
        binance_raw_prices AS brp 
    WINDOW window_1m AS (
        PARTITION BY ticker,
            date_trunc('minute', created_at)
        ORDER BY
            created_at ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
),
calculated_candles AS (
    SELECT 
        ticker,
        m1_bucket AS bucket,
        open_price,
        MAX(price) AS high_price,
        MIN(price) AS low_price,
        close_price,
        COUNT(*) AS ticks_count
    FROM windowed_values
    GROUP BY ticker, m1_bucket, open_price, close_price
)
--
INSERT INTO binance_candles_1m (
    ticker, 
    bucket, 
    open_price, 
    high_price, 
    low_price, 
    close_price, 
    ticks_count
)
--
SELECT 
    ticker, 
    bucket, 
    open_price, 
    high_price, 
    low_price, 
    close_price, 
    ticks_count
FROM calculated_candles
ON CONFLICT (ticker, bucket) DO UPDATE SET
    open_price = EXCLUDED.open_price,
    high_price = EXCLUDED.high_price,
    low_price = EXCLUDED.low_price,
    close_price = EXCLUDED.close_price,
    ticks_count = EXCLUDED.ticks_count,
    created_at = CURRENT_TIMESTAMP;


SELECT * FROM binance_candles_1m AS bcm LIMIT 100;

-- OHLCV
--
-- open
-- high
-- low
-- close
-- volume
--

SELECT
    *
FROM
    binance_raw_prices AS brp
LIMIT 10;