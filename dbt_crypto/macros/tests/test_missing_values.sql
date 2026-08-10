{% test missing_candles(model, symbol_column='symbol', bucket_column='bucket', interval_value='2 hours', step_interval='1 minute') %}

WITH global_max AS (
    -- Находим единую точку отсчета по проверяемой модели
    SELECT date_trunc('minute', MAX({{ bucket_column }})) AS max_b
    FROM {{ model }}
),
grid AS (
    -- Генерируем параметризованную сетку времени с единым синтаксисом INTERVAL
    SELECT generate_series(
        max_b - INTERVAL '{{ interval_value }}',
        max_b,
        INTERVAL '{{ step_interval }}'
    ) AS expected_bucket
    FROM global_max
),
expected_buckets AS (
    -- Делаем Декартово произведение сетки и списка уникальных символов
    SELECT
        t.{{ symbol_column }},
        g.expected_bucket
    FROM (SELECT DISTINCT {{ symbol_column }} FROM {{ model }}) t
    CROSS JOIN grid g
)
-- Находим пропущенные бакеты в целевой модели и сортируем результат
SELECT
    eb.{{ symbol_column }},
    eb.expected_bucket AS missing_bucket,
    'MISSING_CANDLE' AS issue
FROM expected_buckets eb
LEFT JOIN {{ model }} c
    ON eb.{{ symbol_column }} = c.{{ symbol_column }}
   AND eb.expected_bucket = c.{{ bucket_column }}
WHERE c.{{ bucket_column }} IS NULL
ORDER BY eb.{{ symbol_column }}, eb.expected_bucket DESC

{% endtest %}
