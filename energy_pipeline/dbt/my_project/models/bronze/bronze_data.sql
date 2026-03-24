{{ config(materialized='incremental', unique_key='api_id') }}

with source as (
    select * from {{ source('staging','staging') }}
    {% if is_incremental() %}
        where ingested_at > (select coalesce(max(ingested_at), '1990-01-01') from {{ this }})
    {% endif %}
),

bronze as (
    select
        id as staging_id,
        (raw_payload->>'_i')::integer as internal_id,
        (raw_payload->>'id')::varchar as api_id,
        (raw_payload->>'_rand')::bigint as internal_rand,
        (raw_payload->>'_score')::float as score,
        ((raw_payload->>'horodate')::timestamp with time zone) at time zone 'Europe/Paris' at time zone 'UTC' as horodate,
        (raw_payload->>'mois')::integer as mois,
        (raw_payload->>'pertes')::bigint as pertes,
        (raw_payload->>'injection_rte')::bigint as injection_rte,
        (raw_payload->>'soutirage_rte')::bigint as soutirage_rte,
        (raw_payload->>'consommation_hta')::bigint as consommation_hta,
        (raw_payload->>'production_autre')::bigint as production_autre,
        (raw_payload->>'production_eolien')::bigint as production_eolien,
        (raw_payload->>'production_totale')::bigint as production_totale,
        (raw_payload->>'pseudo_rayonnement')::integer as pseudo_rayonnement,
        (raw_payload->>'consommation_totale')::bigint as consommation_totale,
        (raw_payload->>'production_profilee')::bigint as production_profilee,
        (raw_payload->>'production_telerelevee')::bigint as production_telerelevee,
        (raw_payload->>'production_cogeneration')::bigint as production_cogeneration,
        (raw_payload->>'production_profilee_aut')::bigint as production_profilee_aut,
        (raw_payload->>'consommation_profilee_pro')::bigint as consommation_profilee_pro,
        (raw_payload->>'consommation_profilee_res')::bigint as consommation_profilee_res,
        (raw_payload->>'production_photovoltaique')::bigint as production_photovoltaique,
        (raw_payload->>'soutirage_vers_autres_grd')::bigint as soutirage_vers_autres_grd,
        (raw_payload->>'temperature_reelle_lissee')::float              as temperature_reelle_lissee,
        (raw_payload->>'temperature_normale_lissee')::float             as temperature_normale_lissee,
        (raw_payload->>'consommation_profilee_ent_bt')::bigint          as consommation_profilee_ent_bt,
        (raw_payload->>'consommation_telerelevee_hta')::bigint          as consommation_telerelevee_hta,
        (raw_payload->>'consommation_profilee_ent_hta')::bigint         as consommation_profilee_ent_hta,
        (raw_payload->>'consommation_telerelevee_btsup')::bigint        as consommation_telerelevee_btsup,
        (raw_payload->>'production_profilee_hydraulique')::bigint       as production_profilee_hydraulique,
        (raw_payload->>'production_profilee_cogeneration')::bigint      as production_profilee_cogeneration,
        (raw_payload->>'production_profilee_photovoltaique')::bigint    as production_profilee_photovoltaique,
        (raw_payload->>'consommation_telerelevee_residentielle')::bigint        as consommation_telerelevee_residentielle,
        (raw_payload->>'consommation_telerelevee_professionnelle')::bigint      as consommation_telerelevee_professionnelle,
        ingested_at
    from source

)

select * from bronze