{{ config(materialized='incremental', unique_key='timestamp') }}

with bronze as (
    select * from {{ ref('bronze_rte_spot_prices') }}
    {% if is_incremental() %}
        where timestamp > (select coalesce(max(timestamp), '1990-01-01') from {{ this }})
    {% endif %}
)

select
    timestamp,
    extract(month from timestamp)       as month,
    extract(year from timestamp)        as year,
    price_eur_mwh,
    source_date,
    periode
from bronze