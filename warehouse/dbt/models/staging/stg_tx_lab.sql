-- stg_tx_lab.sql
-- Staging model for Texas Dental Laboratory license data

{{ config(
    materialized='view',
    schema='staging'
) }}

with source as (
    select * from {{ source('texas_raw', 'labs') }}
),

renamed as (
    select
        -- Source identification
        'TX' as state_code,
        'dental_lab' as professional_type,

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
            when lic_sta_cde in (61, 65) then 'INACTIVE'
            when lic_sta_cde = 80 then 'CLOSED'  -- "Closed" for labs, not "Deceased"
            else 'UNKNOWN'
        end as status_category,

        -- Dates
        try_to_date(lic_orig_dte, 'MM/DD/YYYY') as license_issue_date,
        try_to_date(lic_expr_dte, 'MM/DD/YYYY') as license_expiration_date,

        -- Entity info (labs have business name, not personal name)
        trim(lab_nme) as business_name,
        trim(lab_owner) as owner_name,
        trim(lab_manager) as manager_name,
        trim(lab_cdt) as certified_dental_technician,

        -- Lab type
        trim(lab_type) as lab_type,

        -- Address
        trim(address1) as address_line_1,
        trim(address2) as address_line_2,
        trim(city) as city,
        trim(state) as address_state,
        trim(zip) as zip_code,
        trim(county) as county,
        trim(country) as country,
        trim(phone) as phone,

        -- Disciplinary
        upper(trim(disc_action)) = 'YES' as has_disciplinary_action,
        upper(trim(remedial_plns)) = 'YES' as has_remedial_plans,

        -- Metadata
        current_timestamp() as _loaded_at

    from source
)

select * from renamed
