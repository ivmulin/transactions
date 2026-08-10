{% test ohlc_candle(
    model,
    symbol='symbol',
    bucket='bucket',
    open_price_column='open_price',
    high_price_column='high_price',
    low_price_column='low_price',
    close_price_column='close_price',
    ticks_count_column='ticks_count'
) %}

SELECT
    {{ symbol }},
    {{ bucket }},
    {{ open_price_column }},
    {{ high_price_column }},
    {{ low_price_column }},
    {{ close_price_column }},
    {{ ticks_count_column }}
FROM {{ model }}
WHERE
    -- High должен быть не меньше всех остальных цен
    {{ high_price_column }} < {{ open_price_column }}
    OR {{ high_price_column }} < {{ close_price_column }}
    OR {{ high_price_column }} < {{ low_price_column }}
    -- Low должен быть не больше всех остальных цен
    OR {{ low_price_column }} > {{ open_price_column }}
    OR {{ low_price_column }} > {{ close_price_column }}
    -- Цены и количество тиков должны быть строго положительными
    OR {{ low_price_column }} <= 0
    OR {{ ticks_count_column }} <= 0

{% endtest %}
