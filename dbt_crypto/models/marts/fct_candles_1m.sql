{{
    config(
        materialized='incremental',
        unique_key=['symbol', 'bucket'],
        incremental_strategy='merge'
    )
}}
WITH
  windowed_values AS (
    SELECT
      symbol,
      price,
      date_trunc('minute', ingested_at) AS m1_bucket,
      FIRST_VALUE(price) OVER window_1m AS open_price,
      LAST_VALUE(price) OVER window_1m AS close_price
    FROM
      {{ ref('stg_binance__raw_prices') }}
    {%- if is_incremental() -%}
    WHERE ingested_at >= (SELECT MAX(bucket) FROM {{ this }}) - INTERVAL '10 minutes'
    {% endif %}
    WINDOW
      window_1m AS (
        PARTITION BY
          symbol,
          date_trunc('minute', ingested_at)
        ORDER BY
          ingested_at ASC ROWS BETWEEN UNBOUNDED PRECEDING
          AND UNBOUNDED FOLLOWING
      )
  ),
  calculated_candles AS (
      SELECT
          symbol,
          m1_bucket AS bucket,
          open_price,
          MAX(price) AS high_price,
          MIN(price) AS low_price,
          close_price,
          COUNT(*) AS ticks_count
      FROM windowed_values
      GROUP BY symbol, m1_bucket, open_price, close_price
  )
SELECT * FROM calculated_candles
