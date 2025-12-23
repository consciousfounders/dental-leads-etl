-- dim_dental_professionals.sql
-- Current state dimension table for all dental professionals
-- Used for CRM syncs, audience building, and analysis

{{ config(
    materialized='table',
    schema='marts'
) }}

with current_snapshot as (
    select *
    from {{ ref('snp_dental_professionals') }}
    where dbt_valid_to is null  -- Current records only
),

enriched as (
    select
        professional_id,
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

        -- Calculate license tenure
        datediff(year, license_issue_date, current_date()) as years_licensed,
        datediff(day, current_date(), license_expiration_date) as days_until_expiration,
        case
            when license_expiration_date < current_date() then 'EXPIRED'
            when license_expiration_date < dateadd(day, 30, current_date()) then 'EXPIRING_30_DAYS'
            when license_expiration_date < dateadd(day, 90, current_date()) then 'EXPIRING_90_DAYS'
            else 'VALID'
        end as expiration_status,

        -- Personal
        first_name,
        middle_name,
        last_name,
        concat(first_name, ' ', last_name) as full_name,
        former_last_name,
        gender,
        birth_year,
        case
            when birth_year is not null
            then year(current_date()) - cast(birth_year as integer)
        end as approximate_age,
        graduation_year,
        case
            when graduation_year is not null
            then year(current_date()) - cast(graduation_year as integer)
        end as years_since_graduation,
        school_name,

        -- Address
        address_line_1,
        address_line_2,
        city,
        address_state,
        zip_code,
        left(zip_code, 5) as zip5,
        county,
        country,
        phone,

        -- Practice
        practice_description,
        specialty_code,
        specialty_description,

        -- Certifications - individual flags
        has_nitrous_oxide_permit,
        has_anesthesia_level_1,
        has_anesthesia_level_2,
        has_anesthesia_level_3,
        has_anesthesia_level_4,
        has_portability,
        has_sealant_certification,
        has_nitrous_monitoring,
        has_local_infiltration_anesthesia,

        -- Certification summary
        case
            when has_anesthesia_level_4 then 'LEVEL_4'
            when has_anesthesia_level_3 then 'LEVEL_3'
            when has_anesthesia_level_2 then 'LEVEL_2'
            when has_anesthesia_level_1 then 'LEVEL_1'
            when has_nitrous_oxide_permit then 'NITROUS_ONLY'
            else 'NONE'
        end as sedation_level,

        -- Count of certifications for segmentation
        (
            case when has_nitrous_oxide_permit then 1 else 0 end +
            case when has_anesthesia_level_1 then 1 else 0 end +
            case when has_anesthesia_level_2 then 1 else 0 end +
            case when has_anesthesia_level_3 then 1 else 0 end +
            case when has_anesthesia_level_4 then 1 else 0 end +
            case when has_sealant_certification then 1 else 0 end +
            case when has_nitrous_monitoring then 1 else 0 end +
            case when has_local_infiltration_anesthesia then 1 else 0 end
        ) as certification_count,

        -- Disciplinary
        has_disciplinary_action,
        has_remedial_plans,

        -- Segmentation helpers
        case
            when professional_type = 'dentist' and specialty_code in ('GEN', '', null) then 'General Dentist'
            when professional_type = 'dentist' then 'Specialist'
            when professional_type = 'hygienist' then 'Hygienist'
            when professional_type = 'dental_assistant' then 'Assistant'
            else 'Other'
        end as professional_segment,

        -- Marketability score (simple version)
        case
            when status_category != 'ACTIVE' then 0
            when has_disciplinary_action then 1
            when professional_type = 'dentist' and has_anesthesia_level_4 then 5
            when professional_type = 'dentist' and specialty_code in ('OMS', 'PERI', 'ORTH') then 4
            when professional_type = 'dentist' then 3
            when professional_type = 'hygienist' then 2
            else 1
        end as marketing_priority_score,

        -- Metadata
        _loaded_at,
        dbt_valid_from as record_valid_from,
        current_timestamp() as record_updated_at

    from current_snapshot
)

select * from enriched
