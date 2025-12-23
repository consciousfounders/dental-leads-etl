-- stg_tx_hygienist.sql
-- Staging model for Texas Hygienist license data

{{ config(
    materialized='view',
    schema='staging'
) }}

with source as (
    select * from {{ source('texas_raw', 'hygienist') }}
),

renamed as (
    select
        -- Source identification
        'TX' as state_code,
        'hygienist' as professional_type,

        -- Primary identifiers
        cast(lic_id as integer) as source_license_id,
        trim(lic_nbr) as license_number,
        cast(entity_nbr as integer) as entity_number,

        -- License status
        cast(lic_sta_cde as integer) as status_code,
        trim(lic_sta_desc) as status_description,
        case
            when lic_sta_cde in (20, 46) then 'ACTIVE'
            when lic_sta_cde in (45, 48, 60) then 'LAPSED'
            when lic_sta_cde in (61, 65, 72) then 'INACTIVE'
            when lic_sta_cde = 80 then 'DECEASED'
            when lic_sta_cde = 47 then 'SUSPENDED'
            else 'UNKNOWN'
        end as status_category,

        -- Dates
        try_to_date(lic_orig_dte, 'MM/DD/YYYY') as license_issue_date,
        try_to_date(lic_expr_dte, 'MM/DD/YYYY') as license_expiration_date,

        -- Personal info (note: LAST_MNE is a typo in source data)
        trim(first_nme) as first_name,
        trim(middle_nme) as middle_name,
        trim(last_mne) as last_name,  -- Source has typo: LAST_MNE
        trim(former_last_nme) as former_last_name,
        trim(gender) as gender,
        nullif(trim(birth_year), '') as birth_year,

        -- Education
        nullif(trim(grad_yr), '') as graduation_year,
        trim(school) as school_name,

        -- Address
        trim(address1) as address_line_1,
        trim(address2) as address_line_2,
        trim(city) as city,
        trim(state) as address_state,
        trim(zip) as zip_code,
        trim(county) as county,
        trim(country) as country,
        trim(phone) as phone,

        -- Practice info (hygienists don't have specialty)
        null as practice_description,
        null as specialty_code,
        null as specialty_description,

        -- Certifications (Hygienist-specific)
        null as nitrous_oxide_permit_date,
        null as anesthesia_level_1_date,
        null as anesthesia_level_2_date,
        null as anesthesia_level_3_date,
        null as anesthesia_level_4_date,

        -- Certification flags (Hygienist-specific)
        upper(trim(sealant)) = 'YES' as has_sealant_certification,
        upper(trim(nom_mod)) = 'YES' as has_nitrous_monitoring,
        upper(trim(lia_mod)) = 'YES' as has_local_infiltration_anesthesia,
        false as has_nitrous_oxide_permit,
        false as has_anesthesia_level_1,
        false as has_anesthesia_level_2,
        false as has_anesthesia_level_3,
        false as has_anesthesia_level_4,
        false as has_portability,
        false as has_shrp_modifier,
        false as has_spp_modifier,
        false as has_erx_waiver,
        false as is_level_exempt,

        -- Disciplinary
        upper(trim(disc_action)) = 'YES' as has_disciplinary_action,
        upper(trim(remedial_plns)) = 'YES' as has_remedial_plans,

        -- Metadata
        current_timestamp() as _loaded_at

    from source
)

select * from renamed
