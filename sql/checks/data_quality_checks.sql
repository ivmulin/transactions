-- ====================================================================
-- 1. СВЕЖЕСТЬ ДАННЫХ (Data Freshness / Lag Check)
-- Считаем отставание (в секундах и минутах) между текущим временем 
-- и последней свечой по каждому тикеру.
-- ====================================================================
--title: data freshness
SELECT 
    ticker,
    MAX(bucket) AS max_bucket,
    NOW() AS current_dwh_time,
    ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(bucket)))) AS lag_seconds,
    ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(bucket))) / 60, 2) AS lag_minutes,
    CASE 
        WHEN EXTRACT(EPOCH FROM (NOW() - MAX(bucket))) > 180 THEN 'CRITICAL: Lag > 3m'
        WHEN EXTRACT(EPOCH FROM (NOW() - MAX(bucket))) > 120 THEN 'WARNING: Lag > 2m'
        ELSE 'OK'
    END AS status
FROM binance_candles_1m
GROUP BY ticker
ORDER BY lag_seconds DESC;
--
-- ====================================================================
-- 2. ВАЛИДАЦИЯ ЦЕН И СВЕЧЕЙ (Price / OHLC Invariant Validation)
-- Находим аномальные свечи, где нарушена геометрия OHLC или цена <= 0.
-- В идеале этот запрос должен возвращать 0 строк!
-- ====================================================================
-- title: anomalies
SELECT 
    ticker,
    bucket,
    open_price,
    high_price,
    low_price,
    close_price,
    ticks_count
FROM binance_candles_1m
WHERE 
    -- High должен быть не меньше всех остальных цен
    high_price < open_price 
    OR high_price < close_price
    OR high_price < low_price
    -- Low должен быть не больше всех остальных цен
    OR low_price > open_price 
    OR low_price > close_price
    -- Цены и количество тиков должны быть строго положительными
    OR low_price <= 0
    OR ticks_count <= 0;
--
-- ====================================================================
-- 3. ПОИСК ПРОПУСКОВ (Gaps Detection) — ФИНАЛЬНЫЙ РАБОЧИЙ ВАРИАНТ
-- ====================================================================
-- title: missing candle values
WITH global_max AS (
    -- Находим единую точку отсчета по всей витрине
    SELECT date_trunc('minute', MAX(bucket)) AS max_b 
    FROM binance_candles_1m
),
grid AS (
    -- Генерируем 2 часа сетки
    SELECT generate_series(
        max_b - INTERVAL '2 hours',
        max_b,
        INTERVAL '1 minute'
    ) AS expected_bucket
    FROM global_max
),
expected_buckets AS (
    -- Делаем Декартово произведение сетки и списка тикеров
    SELECT 
        t.ticker,
        g.expected_bucket
    FROM (SELECT DISTINCT ticker FROM binance_candles_1m) t
    CROSS JOIN grid g
)
SELECT 
    eb.ticker,
    eb.expected_bucket AS missing_bucket,
    'MISSING_CANDLE' AS issue
FROM expected_buckets eb
LEFT JOIN binance_candles_1m c 
    ON eb.ticker = c.ticker 
   AND eb.expected_bucket = c.bucket
WHERE c.bucket IS NULL
ORDER BY eb.ticker, eb.expected_bucket DESC;