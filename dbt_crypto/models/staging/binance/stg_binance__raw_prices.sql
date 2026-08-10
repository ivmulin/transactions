WITH extern_binance_data AS (
    SELECT * FROM {{ source('binance_raw', 'binance_raw_prices') }}
),
renamed_data AS (
    SELECT
        id,
        ticker AS symbol,
        CAST(price AS NUMERIC(18, 8)) AS price,
        created_at AS ingested_at
    FROM extern_binance_data
)
SELECT * FROM renamed_data
