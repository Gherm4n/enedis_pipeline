{{ config(materialized='incremental', unique_key='timestamp') }}

with source as (
    select * from {{ source('staging', 'rte_spot_prices') }}
    {% if is_incremental() %}
        where timestamp > (select coalesce(max(timestamp), '1990-01-01') from {{ this }})
    {% endif %}
)

select
    timestamp,
    price_eur_mwh,
    source_date,
    periode
from source