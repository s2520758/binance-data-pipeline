{{
    config(
        materialized='incremental',
        unique_key='trade_id',
        partition_by={
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

select
    trade_id,
    symbol,
    event_time,
    event_date,
    price,
    quantity,
    (price * quantity) as total_notional_usd,
    current_timestamp() as dbt_updated_at

from {{ ref('stg_binance_trades') }}

{% if is_incremental() %}
  where event_time > (select max(event_time) from {{ this }})
{% endif %}