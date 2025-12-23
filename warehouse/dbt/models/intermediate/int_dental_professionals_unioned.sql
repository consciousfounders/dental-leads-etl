-- int_dental_professionals_unioned.sql
-- Unified model for all dental professional types
-- This creates a consistent schema across dentists, hygienists, and assistants

{{ config(
    materialized='view',
    schema='intermediate'
) }}

with dentists as (
    select
        -- Composite key for multi-state tracking
        {{ dbt_utils.generate_surrogate_key(['state_code', 'professional_type', 'license_number']) }} as professional_id,

        state_code,
        professional_type,
        source_license_id,
        license_number,
        entity_number,

        -- Status
        status_code,
        status_description,
        status_category,
        license_issue_date,
        license_expiration_date,

        -- Personal
        first_name,
        middle_name,
        last_name,
        former_last_name,
        gender,
        birth_year,
        graduation_year,
        school_name,

        -- Address
        address_line_1,
        address_line_2,
        city,
        address_state,
        zip_code,
        county,
        country,
        phone,

        -- Practice
        practice_description,
        specialty_code,
        specialty_description,

        -- Certifications (all types combined)
        has_nitrous_oxide_permit,
        has_anesthesia_level_1,
        has_anesthesia_level_2,
        has_anesthesia_level_3,
        has_anesthesia_level_4,
        has_portability,
        false as has_sealant_certification,
        false as has_nitrous_monitoring,
        false as has_local_infiltration_anesthesia,

        -- Disciplinary
        has_disciplinary_action,
        has_remedial_plans,

        _loaded_at

    from {{ ref('stg_tx_dentist') }}
),

hygienists as (
    select
        {{ dbt_utils.generate_surrogate_key(['state_code', 'professional_type', 'license_number']) }} as professional_id,

        state_code,
        professional_type,
        source_license_id,
        license_number,
        entity_number,

        status_code,
        status_description,
        status_category,
        license_issue_date,
        license_expiration_date,

        first_name,
        middle_name,
        last_name,
        former_last_name,
        gender,
        birth_year,
        graduation_year,
        school_name,

        address_line_1,
        address_line_2,
        city,
        address_state,
        zip_code,
        county,
        country,
        phone,

        null as practice_description,
        null as specialty_code,
        null as specialty_description,

        false as has_nitrous_oxide_permit,
        false as has_anesthesia_level_1,
        false as has_anesthesia_level_2,
        false as has_anesthesia_level_3,
        false as has_anesthesia_level_4,
        false as has_portability,
        has_sealant_certification,
        has_nitrous_monitoring,
        has_local_infiltration_anesthesia,

        has_disciplinary_action,
        has_remedial_plans,

        _loaded_at

    from {{ ref('stg_tx_hygienist') }}
),

assistants as (
    select
        {{ dbt_utils.generate_surrogate_key(['state_code', 'professional_type', 'license_number']) }} as professional_id,

        state_code,
        professional_type,
        source_license_id,
        license_number,
        entity_number,

        status_code,
        status_description,
        status_category,
        license_issue_date,
        license_expiration_date,

        first_name,
        middle_name,
        last_name,
        null as former_last_name,
        gender,
        birth_year,
        null as graduation_year,
        null as school_name,

        address_line_1,
        address_line_2,
        city,
        address_state,
        zip_code,
        county,
        country,
        phone,

        null as practice_description,
        null as specialty_code,
        null as specialty_description,

        false as has_nitrous_oxide_permit,
        false as has_anesthesia_level_1,
        false as has_anesthesia_level_2,
        false as has_anesthesia_level_3,
        false as has_anesthesia_level_4,
        false as has_portability,
        false as has_sealant_certification,
        has_nitrous_monitoring,
        false as has_local_infiltration_anesthesia,

        has_disciplinary_action,
        has_remedial_plans,

        _loaded_at

    from {{ ref('stg_tx_dental_assistant') }}
)

select * from dentists
union all
select * from hygienists
union all
select * from assistants
