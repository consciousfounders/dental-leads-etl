-- stg_tx_dentist.sql
-- Staging model for Texas Dentist license data
-- Standardizes column names and types before loading to unified model

{{ config(
    materialized='view',
    schema='staging'
) }}

with source as (
    select * from {{ source('texas_raw', 'dentist') }}
),

renamed as (
    select
        -- Source identification
        'TX' as state_code,
        'dentist' as professional_type,

        -- Primary identifiers
        cast(lic_id as integer) as source_license_id,
        trim(lic_nbr) as license_number,
        cast(entity_nbr as integer) as entity_number,

        -- License status
        cast(lic_sta_cde as integer) as status_code,
        trim(lic_sta_desc) as status_description,
        case
            when lic_sta_cde in (20, 46, 70) then 'ACTIVE'
            when lic_sta_cde in (45, 48, 60) then 'LAPSED'
            when lic_sta_cde in (61, 65, 71, 72) then 'INACTIVE'
            when lic_sta_cde = 80 then 'DECEASED'
            when lic_sta_cde = 47 then 'SUSPENDED'
            else 'UNKNOWN'
        end as status_category,

        -- Dates
        try_to_date(lic_orig_dte, 'MM/DD/YYYY') as license_issue_date,
        try_to_date(lic_expr_dte, 'MM/DD/YYYY') as license_expiration_date,

        -- Personal info
        trim(first_nme) as first_name,
        trim(middle_nme) as middle_name,
        trim(last_nme) as last_name,
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

        -- Practice info
        trim(prac_desc) as practice_description,
        trim(prac_types) as specialty_code,
        case trim(prac_types)
            when 'GEN' then 'General Dentistry'
            when 'ORTH' then 'Orthodontics'
            when 'OMS' then 'Oral & Maxillofacial Surgery'
            when 'PEDO' then 'Pediatric Dentistry'
            when 'PERI' then 'Periodontics'
            when 'PROS' then 'Prosthodontics'
            when 'END' then 'Endodontics'
            when 'DPH' then 'Dental Public Health'
            when 'OMP' then 'Oral Medicine/Pathology'
            when 'OFPN' then 'Orofacial Pain'
            when 'OMR' then 'Oral & Maxillofacial Radiology'
            when 'ANE' then 'Dental Anesthesiology'
            else null
        end as specialty_description,

        -- Certifications/Permits (Dentist-specific)
        case when nox_permit_dte not in ('No Permit', '')
             then try_to_date(nox_permit_dte, 'MM/DD/YYYY') end as nitrous_oxide_permit_date,
        case when level_1_dte not in ('No Permit', '')
             then try_to_date(level_1_dte, 'MM/DD/YYYY') end as anesthesia_level_1_date,
        case when level_2_dte not in ('No Permit', '')
             then try_to_date(level_2_dte, 'MM/DD/YYYY') end as anesthesia_level_2_date,
        case when level_3_dte not in ('No Permit', '')
             then try_to_date(level_3_dte, 'MM/DD/YYYY') end as anesthesia_level_3_date,
        case when level_4_dte not in ('No Permit', '')
             then try_to_date(level_4_dte, 'MM/DD/YYYY') end as anesthesia_level_4_date,

        -- Certification flags
        nox_permit_dte not in ('No Permit', '') as has_nitrous_oxide_permit,
        level_1_dte not in ('No Permit', '') as has_anesthesia_level_1,
        level_2_dte not in ('No Permit', '') as has_anesthesia_level_2,
        level_3_dte not in ('No Permit', '') as has_anesthesia_level_3,
        level_4_dte not in ('No Permit', '') as has_anesthesia_level_4,
        upper(trim(portability)) = 'YES' as has_portability,
        upper(trim(shrp_mod)) != 'NO' as has_shrp_modifier,
        upper(trim(spp_mod)) != 'NO' as has_spp_modifier,
        upper(trim(erx_waiver)) = 'YES' as has_erx_waiver,
        upper(trim(level_exempt)) = 'YES' as is_level_exempt,

        -- Disciplinary
        upper(trim(disc_action)) = 'YES' as has_disciplinary_action,
        upper(trim(remedial_plns)) = 'YES' as has_remedial_plans,

        -- Metadata
        current_timestamp() as _loaded_at

    from source
)

select * from renamed
