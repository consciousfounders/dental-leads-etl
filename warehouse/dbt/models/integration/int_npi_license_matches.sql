{{
    config(
        materialized='table',
        schema='integration'
    )
}}

/*
    NPI to State License Matching

    Matches NPI records to state license records using fuzzy matching.
    Produces a confidence score for each potential match.

    Match hierarchy:
    1. Exact NPI (if license file has it) - 100
    2. Name + City + State exact - 95
    3. Name + ZIP5 exact - 90
    4. Name (soundex) + City + State - 85
    5. Name (soundex) + ZIP5 - 80
    6. Name (soundex) + State only - 70
*/

WITH npi_dentists AS (
    -- NPI records filtered to dental taxonomy codes
    SELECT
        npi AS npi_number,
        UPPER(TRIM(provider_first_name)) AS first_name,
        UPPER(TRIM(provider_last_name_legal_name)) AS last_name,
        SOUNDEX(provider_first_name) AS first_name_soundex,
        SOUNDEX(provider_last_name_legal_name) AS last_name_soundex,

        -- Practice location (preferred)
        UPPER(TRIM(provider_business_practice_location_address_city_name)) AS practice_city,
        UPPER(TRIM(provider_business_practice_location_address_state_name)) AS practice_state,
        LEFT(provider_business_practice_location_address_postal_code, 5) AS practice_zip5,

        -- Mailing location (fallback)
        UPPER(TRIM(provider_first_line_business_mailing_address)) AS mailing_address,
        UPPER(TRIM(provider_business_mailing_address_city_name)) AS mailing_city,
        UPPER(TRIM(provider_business_mailing_address_state_name)) AS mailing_state,
        LEFT(provider_business_mailing_address_postal_code, 5) AS mailing_zip5,

        -- Contact info (the prize!)
        provider_business_practice_location_address_telephone_number AS phone,

        healthcare_provider_taxonomy_code_1 AS taxonomy_code,
        enumeration_date AS npi_enumeration_date

    FROM {{ source('raw_npi', 'npi_providers') }}
    WHERE healthcare_provider_taxonomy_code_1 IN (
        '1223G0001X',  -- General Practice Dentist
        '122300000X',  -- Dentist
        '1223D0001X',  -- Dental Public Health
        '1223E0200X',  -- Endodontist
        '1223P0106X',  -- Oral and Maxillofacial Pathology
        '1223D0008X',  -- Oral and Maxillofacial Radiology
        '1223S0112X',  -- Oral and Maxillofacial Surgery
        '1223X0400X',  -- Orthodontics
        '1223P0221X',  -- Pediatric Dentistry
        '1223P0300X',  -- Periodontics
        '1223P0700X',  -- Prosthodontics
        '124Q00000X',  -- Dental Hygienist
        '126800000X'   -- Dental Assistant (rarely has NPI)
    )
    AND entity_type_code = '1'  -- Individual providers only
    AND npi_deactivation_date IS NULL  -- Active NPIs only
),

tx_licenses AS (
    -- Texas license records
    SELECT
        'TX' AS state_code,
        license_number,
        professional_type,
        UPPER(TRIM(first_name)) AS first_name,
        UPPER(TRIM(last_name)) AS last_name,
        SOUNDEX(first_name) AS first_name_soundex,
        SOUNDEX(last_name) AS last_name_soundex,
        UPPER(TRIM(city)) AS city,
        UPPER(TRIM(state)) AS state,
        LEFT(zip_code, 5) AS zip5,
        UPPER(TRIM(county)) AS county,
        license_status,
        license_original_date,
        license_expiration_date,
        school,
        graduation_year,
        specialty_codes
    FROM {{ ref('stg_tx_dentist') }}
    WHERE license_status_code IN (20, 46, 70)  -- Active statuses

    UNION ALL

    SELECT
        'TX' AS state_code,
        license_number,
        professional_type,
        UPPER(TRIM(first_name)) AS first_name,
        UPPER(TRIM(last_name)) AS last_name,
        SOUNDEX(first_name) AS first_name_soundex,
        SOUNDEX(last_name) AS last_name_soundex,
        UPPER(TRIM(city)) AS city,
        UPPER(TRIM(state)) AS state,
        LEFT(zip_code, 5) AS zip5,
        UPPER(TRIM(county)) AS county,
        license_status,
        license_original_date,
        license_expiration_date,
        school,
        graduation_year,
        NULL AS specialty_codes
    FROM {{ ref('stg_tx_hygienist') }}
    WHERE license_status_code IN (20, 46, 70)
),

-- Matching logic
matches AS (
    SELECT
        lic.state_code,
        lic.license_number,
        lic.professional_type,
        lic.first_name AS lic_first_name,
        lic.last_name AS lic_last_name,
        lic.city AS lic_city,
        lic.state AS lic_state,
        lic.zip5 AS lic_zip5,

        npi.npi_number,
        npi.first_name AS npi_first_name,
        npi.last_name AS npi_last_name,
        npi.practice_city AS npi_city,
        npi.practice_state AS npi_state,
        npi.practice_zip5 AS npi_zip5,
        npi.phone AS npi_phone,

        -- Calculate match confidence
        CASE
            -- Exact name + city + state
            WHEN lic.first_name = npi.first_name
                 AND lic.last_name = npi.last_name
                 AND lic.city = npi.practice_city
                 AND lic.state = npi.practice_state
            THEN 95

            -- Exact name + ZIP5
            WHEN lic.first_name = npi.first_name
                 AND lic.last_name = npi.last_name
                 AND lic.zip5 = npi.practice_zip5
            THEN 90

            -- Soundex name + city + state
            WHEN lic.first_name_soundex = npi.first_name_soundex
                 AND lic.last_name_soundex = npi.last_name_soundex
                 AND lic.city = npi.practice_city
                 AND lic.state = npi.practice_state
            THEN 85

            -- Soundex name + ZIP5
            WHEN lic.first_name_soundex = npi.first_name_soundex
                 AND lic.last_name_soundex = npi.last_name_soundex
                 AND lic.zip5 = npi.practice_zip5
            THEN 80

            -- Soundex name + state only (lowest confidence)
            WHEN lic.first_name_soundex = npi.first_name_soundex
                 AND lic.last_name_soundex = npi.last_name_soundex
                 AND lic.state = npi.practice_state
            THEN 70

            ELSE 0
        END AS match_confidence,

        -- Match type description
        CASE
            WHEN lic.first_name = npi.first_name
                 AND lic.last_name = npi.last_name
                 AND lic.city = npi.practice_city
                 AND lic.state = npi.practice_state
            THEN 'exact_name_city_state'

            WHEN lic.first_name = npi.first_name
                 AND lic.last_name = npi.last_name
                 AND lic.zip5 = npi.practice_zip5
            THEN 'exact_name_zip'

            WHEN lic.first_name_soundex = npi.first_name_soundex
                 AND lic.last_name_soundex = npi.last_name_soundex
                 AND lic.city = npi.practice_city
                 AND lic.state = npi.practice_state
            THEN 'soundex_name_city_state'

            WHEN lic.first_name_soundex = npi.first_name_soundex
                 AND lic.last_name_soundex = npi.last_name_soundex
                 AND lic.zip5 = npi.practice_zip5
            THEN 'soundex_name_zip'

            WHEN lic.first_name_soundex = npi.first_name_soundex
                 AND lic.last_name_soundex = npi.last_name_soundex
                 AND lic.state = npi.practice_state
            THEN 'soundex_name_state'

            ELSE 'no_match'
        END AS match_type

    FROM tx_licenses lic
    LEFT JOIN npi_dentists npi
        ON (
            -- Join on soundex to get candidate matches, then score them
            lic.first_name_soundex = npi.first_name_soundex
            AND lic.last_name_soundex = npi.last_name_soundex
            AND lic.state = npi.practice_state
        )
),

-- Deduplicate: keep best match per license
ranked_matches AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY state_code, license_number
            ORDER BY match_confidence DESC, npi_number
        ) AS match_rank
    FROM matches
    WHERE match_confidence > 0  -- Only keep actual matches
)

SELECT
    state_code,
    license_number,
    professional_type,
    lic_first_name,
    lic_last_name,
    lic_city,
    lic_state,
    lic_zip5,
    npi_number,
    npi_first_name,
    npi_last_name,
    npi_city,
    npi_state,
    npi_zip5,
    npi_phone,
    match_confidence,
    match_type,

    -- Flags for workflow
    CASE
        WHEN match_confidence >= 90 THEN FALSE
        WHEN match_confidence >= 70 THEN TRUE
        ELSE FALSE
    END AS needs_manual_review,

    CASE
        WHEN match_confidence >= 85 THEN TRUE
        ELSE FALSE
    END AS auto_approve_for_email,

    CASE
        WHEN match_confidence >= 95 THEN TRUE
        ELSE FALSE
    END AS auto_approve_for_mail,

    CURRENT_TIMESTAMP() AS matched_at

FROM ranked_matches
WHERE match_rank = 1  -- Best match only

UNION ALL

-- Include unmatched licenses (no NPI found)
SELECT
    lic.state_code,
    lic.license_number,
    lic.professional_type,
    lic.first_name AS lic_first_name,
    lic.last_name AS lic_last_name,
    lic.city AS lic_city,
    lic.state AS lic_state,
    lic.zip5 AS lic_zip5,
    NULL AS npi_number,
    NULL AS npi_first_name,
    NULL AS npi_last_name,
    NULL AS npi_city,
    NULL AS npi_state,
    NULL AS npi_zip5,
    NULL AS npi_phone,
    0 AS match_confidence,
    'no_match' AS match_type,
    FALSE AS needs_manual_review,
    FALSE AS auto_approve_for_email,
    FALSE AS auto_approve_for_mail,
    CURRENT_TIMESTAMP() AS matched_at
FROM tx_licenses lic
WHERE NOT EXISTS (
    SELECT 1 FROM ranked_matches rm
    WHERE rm.license_number = lic.license_number
      AND rm.state_code = lic.state_code
      AND rm.match_rank = 1
)
