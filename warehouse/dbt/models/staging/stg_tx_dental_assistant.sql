-- stg_tx_dental_assistant.sql
-- Staging model for Texas Dental Assistant license data

{{ config(
    materialized='view',
    schema='staging'
) }}

with source as (
    select * from {{ source('texas_raw', 'dental_assistant') }}
),

renamed as (
    select
        -- Source identification
        'TX' as state_code,
        'dental_assistant' as professional_type,

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
            when lic_sta_cde in (61, 65, 71, 27) then 'INACTIVE'  -- 71, 27 are both Retired codes
            when lic_sta_cde = 80 then 'DECEASED'
            when lic_sta_cde = 47 then 'SUSPENDED'
            else 'UNKNOWN'
        end as status_category,

        -- Dates
        try_to_date(lic_orig_dte, 'MM/DD/YYYY') as license_issue_date,
        try_to_date(lic_expr_dte, 'MM/DD/YYYY') as license_expiration_date,

        -- Personal info (no former name for assistants)
        trim(first_nme) as first_name,
        trim(middle_nme) as middle_name,
        trim(last_nme) as last_name,
        null as former_last_name,
        trim(gender) as gender,
        nullif(trim(birth_year), '') as birth_year,

        -- Education (assistants don't have school/grad year)
        null as graduation_year,
        null as school_name,

        -- Address
        trim(address1) as address_line_1,
        trim(address2) as address_line_2,
        trim(city) as city,
        trim(state) as address_state,
        trim(zip) as zip_code,
        trim(county) as county,
        trim(country) as country,
        trim(phone) as phone,

        -- Practice info
        null as practice_description,
        null as specialty_code,
        null as specialty_description,

        -- Certifications
        null as nitrous_oxide_permit_date,
        null as anesthesia_level_1_date,
        null as anesthesia_level_2_date,
        null as anesthesia_level_3_date,
        null as anesthesia_level_4_date,

        -- Certification flags (Assistant-specific)
        false as has_sealant_certification,
        upper(trim(nom_mod)) = 'YES' as has_nitrous_monitoring,
        false as has_local_infiltration_anesthesia,
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
