{{ config(materialized='table') }}

with enedis as (
    select * from {{ ref('silver_electricite') }}
),

prices as (
    select * from {{ ref('silver_prices') }}
),

joined as (
    select
        e.horodate                                                                   as timestamp,
        e.mois,
        e.annee,
        e.production_totale,
        e.production_photovoltaique,
        e.production_eolien,
        e.production_cogeneration,
        e.production_autre,
        e.temperature_reelle_lissee,
        e.temperature_normale_lissee,
        e.temperature_reelle_lissee - e.temperature_normale_lissee                  as temperature_deviation,
        e.consommation_totale,
        p.price_eur_mwh,
        e.consommation_totale - e.production_totale                                 as net_import
    from enedis e
    inner join prices p
        on e.horodate = p.timestamp
)

select * from joined