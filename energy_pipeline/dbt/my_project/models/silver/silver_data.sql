with bronze as (
    select * from {{ ref('bronze_data') }}
),

silver as (
    select
        horodate,
        extract(month from horodate) as mois,
        extract(year from horodate) as annee,
        injection_rte,
        soutirage_rte,
        soutirage_vers_autres_grd,
        pertes,
        consommation_totale,
        consommation_hta,
        consommation_telerelevee_hta,
        consommation_telerelevee_btsup,
        consommation_telerelevee_professionnelle,
        consommation_telerelevee_residentielle,
        consommation_profilee_ent_hta,
        consommation_profilee_ent_bt,
        consommation_profilee_pro,
        consommation_profilee_res,
        production_totale,
        production_telerelevee,
        production_photovoltaique,
        production_eolien,
        production_cogeneration,
        production_autre - production_profilee as production_autre,
        production_profilee_photovoltaique,
        production_profilee_cogeneration,
        production_profilee_hydraulique,
        production_profilee_aut,
        temperature_reelle_lissee,
        temperature_normale_lissee,
        pseudo_rayonnement
    from bronze
)

select * from silver