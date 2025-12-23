{{
    config(
        materialized='table',
        schema='integration'
    )
}}

/*
    Golden Provider Record

    Merges license, NPI, and enrichment data into a single "golden" record.

    Data hierarchy (authoritative source per field):

    IDENTITY:
    - Name, license status, dates → State License (authoritative)

    CONTACT:
    - Email → Apollo > Wiza > Scraped (NPI doesn't have email)
    - Phone → NPI > Apollo > Wiza > Scraped
    - LinkedIn → Wiza > Apollo

    ADDRESS:
    - Practice address → NPI preferred (license may be home)
    - Home address → License

    PROFESSIONAL:
    - License details → License only
    - Company/title → Apollo > Wiza > Scraped > NPI

    This is the single source of truth for downstream exports.
*/

WITH matched_records AS (
    SELECT * FROM {{ ref('int_npi_license_matches') }}
),

license_details AS (
    -- Get full license details
    SELECT
        'TX' AS state_code,
        license_number,
        professional_type,
        first_name,
        middle_name,
        last_name,
        gender,
        address1,
        address2,
        city,
        state,
        zip_code,
        county,
        phone,
        license_status,
        license_status_code,
        license_original_date,
        license_expiration_date,
        practice_type,
        specialty_codes,
        school,
        graduation_year,
        birth_year,
        disciplinary_action,
        _load_id,
        _loaded_at
    FROM {{ ref('stg_tx_dentist') }}

    UNION ALL

    SELECT
        'TX' AS state_code,
        license_number,
        professional_type,
        first_name,
        middle_name,
        last_name,
        gender,
        address1,
        address2,
        city,
        state,
        zip_code,
        county,
        phone,
        license_status,
        license_status_code,
        license_original_date,
        license_expiration_date,
        NULL AS practice_type,
        NULL AS specialty_codes,
        school,
        graduation_year,
        birth_year,
        disciplinary_action,
        _load_id,
        _loaded_at
    FROM {{ ref('stg_tx_hygienist') }}
),

npi_details AS (
    -- Get full NPI details for matched records
    SELECT
        npi AS npi_number,
        provider_first_name,
        provider_middle_name,
        provider_last_name_legal_name AS provider_last_name,

        -- Practice address
        provider_first_line_business_practice_location_address AS practice_address1,
        provider_second_line_business_practice_location_address AS practice_address2,
        provider_business_practice_location_address_city_name AS practice_city,
        provider_business_practice_location_address_state_name AS practice_state,
        provider_business_practice_location_address_postal_code AS practice_zip,
        provider_business_practice_location_address_telephone_number AS practice_phone,
        provider_business_practice_location_address_fax_number AS practice_fax,

        -- Mailing address
        provider_first_line_business_mailing_address AS mailing_address1,
        provider_second_line_business_mailing_address AS mailing_address2,
        provider_business_mailing_address_city_name AS mailing_city,
        provider_business_mailing_address_state_name AS mailing_state,
        provider_business_mailing_address_postal_code AS mailing_zip,
        provider_business_mailing_address_telephone_number AS mailing_phone,

        -- Classification
        healthcare_provider_taxonomy_code_1 AS taxonomy_code,
        provider_license_number_1 AS npi_license_number,
        provider_license_number_state_code_1 AS npi_license_state,

        -- Dates
        enumeration_date AS npi_enumeration_date,
        last_update_date AS npi_last_update_date,

        -- Organization affiliation
        provider_organization_name_legal_business_name AS organization_name

    FROM {{ source('raw_npi', 'npi_providers') }}
    WHERE entity_type_code = '1'  -- Individual
),

enrichments AS (
    -- Get enrichment data (email, linkedin, etc.)
    SELECT * FROM {{ ref('int_provider_enrichments') }}
),

golden AS (
    SELECT
        -- Unique provider ID (prefer NPI, fallback to license-based)
        COALESCE(
            m.npi_number,
            m.state_code || '-' || m.license_number
        ) AS provider_id,

        -- Source identifiers
        m.npi_number,
        m.state_code AS license_state,
        m.license_number,

        -- Identity (license is authoritative)
        lic.first_name,
        lic.middle_name,
        lic.last_name,
        CONCAT_WS(' ',
            lic.first_name,
            NULLIF(lic.middle_name, ''),
            lic.last_name
        ) AS full_name,
        lic.gender,

        -- Professional type
        lic.professional_type,
        npi.taxonomy_code,

        -- Contact (enrichment waterfall)
        enr.email,
        enr.email_source,
        COALESCE(enr.phone, npi.practice_phone, lic.phone) AS phone,
        COALESCE(enr.phone_source, CASE WHEN npi.practice_phone IS NOT NULL THEN 'npi' WHEN lic.phone IS NOT NULL THEN 'license' END) AS phone_source,
        npi.practice_fax AS fax,
        enr.linkedin_url,
        enr.linkedin_source,
        enr.website,
        enr.website_source,

        -- Practice address (NPI preferred - license may be home)
        COALESCE(npi.practice_address1, lic.address1) AS address1,
        COALESCE(npi.practice_address2, lic.address2) AS address2,
        COALESCE(npi.practice_city, lic.city) AS city,
        COALESCE(npi.practice_state, lic.state) AS state,
        COALESCE(npi.practice_zip, lic.zip_code) AS zip_code,

        -- License address (for comparison / home address)
        lic.address1 AS license_address1,
        lic.address2 AS license_address2,
        lic.city AS license_city,
        lic.state AS license_state_addr,
        lic.zip_code AS license_zip,
        lic.county,

        -- License details
        lic.license_status,
        lic.license_status_code,
        lic.license_original_date,
        lic.license_expiration_date,
        lic.practice_type,
        lic.specialty_codes,
        lic.disciplinary_action,

        -- Education
        lic.school,
        lic.graduation_year,
        lic.birth_year,

        -- Organization (enrichment > NPI)
        COALESCE(enr.company_name, npi.organization_name) AS company_name,
        COALESCE(enr.company_source, 'npi') AS company_source,
        enr.title,
        enr.employee_count,

        -- Match metadata
        m.match_confidence,
        m.match_type,
        m.needs_manual_review,
        m.auto_approve_for_email,
        m.auto_approve_for_mail,

        -- Enrichment quality score (0-100)
        COALESCE(enr.enrichment_score, 0) AS enrichment_score,

        -- Data quality flags
        CASE WHEN m.npi_number IS NULL THEN TRUE ELSE FALSE END AS missing_npi,
        CASE WHEN enr.email IS NULL THEN TRUE ELSE FALSE END AS missing_email,
        CASE WHEN COALESCE(enr.phone, npi.practice_phone, lic.phone) IS NULL THEN TRUE ELSE FALSE END AS missing_phone,
        CASE WHEN enr.linkedin_url IS NULL THEN TRUE ELSE FALSE END AS missing_linkedin,

        CASE
            WHEN lic.city != npi.practice_city
                 AND m.npi_number IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS address_mismatch,

        -- Is this a new licensee? (within last 180 days)
        CASE
            WHEN lic.license_original_date >= DATEADD(DAY, -180, CURRENT_DATE())
            THEN TRUE
            ELSE FALSE
        END AS is_new_licensee,

        -- Days since licensed
        DATEDIFF(DAY, lic.license_original_date, CURRENT_DATE()) AS days_since_licensed,

        -- Outreach readiness (can we reach them?)
        CASE
            WHEN enr.email IS NOT NULL AND m.match_confidence >= 85 THEN 'email_ready'
            WHEN COALESCE(enr.phone, npi.practice_phone) IS NOT NULL THEN 'phone_only'
            WHEN COALESCE(npi.practice_address1, lic.address1) IS NOT NULL THEN 'mail_only'
            ELSE 'no_contact'
        END AS outreach_readiness,

        -- Source tracking
        lic._load_id,
        lic._loaded_at,
        enr.last_enriched_at,
        CURRENT_TIMESTAMP() AS _golden_created_at

    FROM matched_records m
    JOIN license_details lic
        ON m.license_number = lic.license_number
        AND m.state_code = lic.state_code
    LEFT JOIN npi_details npi
        ON m.npi_number = npi.npi_number
    LEFT JOIN enrichments enr
        ON COALESCE(m.npi_number, m.state_code || '-' || m.license_number) = enr.provider_id
)

SELECT * FROM golden
