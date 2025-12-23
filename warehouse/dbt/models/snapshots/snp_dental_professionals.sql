-- snp_dental_professionals.sql
-- SCD Type 2 snapshot for tracking all changes to dental professional records
-- This enables change detection for license events pipeline

{% snapshot snp_dental_professionals %}

{{
    config(
        target_database='dental_data',
        target_schema='snapshots',
        unique_key='professional_id',
        strategy='check',
        check_cols=[
            -- Status changes (license events)
            'status_code',
            'status_category',
            'license_expiration_date',

            -- Address changes (location events)
            'address_line_1',
            'address_line_2',
            'city',
            'address_state',
            'zip_code',
            'county',

            -- Certification changes (credential events)
            'has_nitrous_oxide_permit',
            'has_anesthesia_level_1',
            'has_anesthesia_level_2',
            'has_anesthesia_level_3',
            'has_anesthesia_level_4',
            'has_sealant_certification',
            'has_nitrous_monitoring',
            'has_local_infiltration_anesthesia',
            'has_portability',

            -- Disciplinary changes
            'has_disciplinary_action',
            'has_remedial_plans',

            -- Practice changes
            'specialty_code',
            'practice_description'
        ],
        invalidate_hard_deletes=True
    )
}}

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

    -- Certifications
    has_nitrous_oxide_permit,
    has_anesthesia_level_1,
    has_anesthesia_level_2,
    has_anesthesia_level_3,
    has_anesthesia_level_4,
    has_portability,
    has_sealant_certification,
    has_nitrous_monitoring,
    has_local_infiltration_anesthesia,

    -- Disciplinary
    has_disciplinary_action,
    has_remedial_plans,

    _loaded_at

from {{ ref('int_dental_professionals_unioned') }}

{% endsnapshot %}
