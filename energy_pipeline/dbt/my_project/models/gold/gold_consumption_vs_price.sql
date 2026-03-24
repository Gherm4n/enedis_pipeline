{{ config(materialized='table') }}

with enedis as (
    select * from {{ ref('silver_electricite') }}
),

prices as (
    select * from {{ ref('silver_rte_spot_prices') }}
),

prices_30min as (
    select
        date_trunc('hour', timestamp) +
            interval '30 min' * floor(extract(minute from timestamp) / 30) as timestamp_30min,
        avg(price_eur_mwh) as avg_price_eur_mwh
    from prices
    group by 1
),

joined as (
    select
        e.horodate                                                                      as timestamp,
        e.mois,
        e.annee,
        e.production_totale,
        e.production_photovoltaique,
        e.production_eolien,
        e.production_cogeneration,
        e.production_autre,
        e.temperature_reelle_lissee,
        e.temperature_normale_lissee,
        e.temperature_reelle_lissee - e.temperature_normale_lissee                     as temperature_deviation,
        e.consommation_totale,
        p.avg_price_eur_mwh                                                             as price_eur_mwh,
        e.consommation_totale - e.production_totale                                     as net_import,
        round((p.avg_price_eur_mwh / nullif(e.consommation_totale, 0))::numeric, 6)    as price_per_unit_consumed
    from enedis e
    left join prices_30min p
        on e.horodate = p.timestamp_30min
)

select * from joined