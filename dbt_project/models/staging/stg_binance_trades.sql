with source as (
    select * from {{ source('binance_source', 'bronze_trades_raw') }}
),

renamed as (
    select
    
        cast(trade_id as string) as trade_id,
        symbol,
        event_time as event_time,
        cast(price as float64) as price,
        cast(quantity as float64) as quantity,
        date(event_time) as event_date
    from source
)

select * from renamed