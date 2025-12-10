-- ============================================================
-- RAW → CLEAN: Dental Providers Transformation
-- ============================================================
-- Filters 9.2M NPI records down to ~260K active US dentists
-- 
-- Filters applied:
--   1. Dental taxonomy codes (1223%) in primary taxonomy
--   2. Active providers (no deactivation date)
--   3. US-based practices only
--   4. Deduplicated on NPI (should be unique anyway)
--
-- Output: ~260K individual dentists + ~105K dental organizations
-- ============================================================

-- Create the main DENTAL_PROVIDERS table
CREATE OR REPLACE TABLE CLEAN.DENTAL_PROVIDERS AS
WITH dental_base AS (
    SELECT
        -- Primary Identifiers
        NPI,
        ENTITY_TYPE_CODE,
        CASE ENTITY_TYPE_CODE 
            WHEN '1' THEN 'Individual'
            WHEN '2' THEN 'Organization'
            ELSE 'Unknown'
        END AS entity_type,
        
        -- Provider Name (Individual)
        NULLIF(TRIM(PROVIDER_FIRST_NAME), '') AS first_name,
        NULLIF(TRIM(PROVIDER_MIDDLE_NAME), '') AS middle_name,
        NULLIF(TRIM(PROVIDER_LAST_NAME), '') AS last_name,
        NULLIF(TRIM(PROVIDER_NAME_PREFIX), '') AS name_prefix,
        NULLIF(TRIM(PROVIDER_NAME_SUFFIX), '') AS name_suffix,
        NULLIF(TRIM(PROVIDER_CREDENTIAL), '') AS credential,
        
        -- Full name construction
        TRIM(
            COALESCE(NULLIF(TRIM(PROVIDER_NAME_PREFIX), ''), '') || ' ' ||
            COALESCE(NULLIF(TRIM(PROVIDER_FIRST_NAME), ''), '') || ' ' ||
            COALESCE(NULLIF(TRIM(PROVIDER_MIDDLE_NAME), ''), '') || ' ' ||
            COALESCE(NULLIF(TRIM(PROVIDER_LAST_NAME), ''), '') || ' ' ||
            COALESCE(NULLIF(TRIM(PROVIDER_NAME_SUFFIX), ''), '')
        ) AS full_name_raw,
        
        -- Organization Name
        NULLIF(TRIM(PROVIDER_ORGANIZATION_NAME), '') AS organization_name,
        NULLIF(TRIM(PROVIDER_OTHER_ORGANIZATION_NAME), '') AS dba_name,
        
        -- Practice Location (primary for outreach)
        NULLIF(TRIM(PROVIDER_FIRST_LINE_BUSINESS_PRACTICE_LOCATION_ADDRESS), '') AS practice_address_1,
        NULLIF(TRIM(PROVIDER_SECOND_LINE_BUSINESS_PRACTICE_LOCATION_ADDRESS), '') AS practice_address_2,
        NULLIF(TRIM(PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_CITY_NAME), '') AS practice_city,
        NULLIF(TRIM(PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_STATE_NAME), '') AS practice_state,
        NULLIF(TRIM(PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_POSTAL_CODE), '') AS practice_zip_raw,
        NULLIF(TRIM(PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_COUNTRY_CODE), '') AS practice_country,
        NULLIF(TRIM(PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_TELEPHONE_NUMBER), '') AS practice_phone_raw,
        NULLIF(TRIM(PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_FAX_NUMBER), '') AS practice_fax_raw,
        
        -- Mailing Address (backup)
        NULLIF(TRIM(PROVIDER_FIRST_LINE_BUSINESS_MAILING_ADDRESS), '') AS mailing_address_1,
        NULLIF(TRIM(PROVIDER_SECOND_LINE_BUSINESS_MAILING_ADDRESS), '') AS mailing_address_2,
        NULLIF(TRIM(PROVIDER_BUSINESS_MAILING_ADDRESS_CITY_NAME), '') AS mailing_city,
        NULLIF(TRIM(PROVIDER_BUSINESS_MAILING_ADDRESS_STATE_NAME), '') AS mailing_state,
        NULLIF(TRIM(PROVIDER_BUSINESS_MAILING_ADDRESS_POSTAL_CODE), '') AS mailing_zip_raw,
        
        -- Taxonomy / Specialty
        NULLIF(TRIM(HEALTHCARE_PROVIDER_TAXONOMY_CODE_1), '') AS primary_taxonomy_code,
        NULLIF(TRIM(HEALTHCARE_PROVIDER_TAXONOMY_CODE_2), '') AS secondary_taxonomy_code,
        NULLIF(TRIM(HEALTHCARE_PROVIDER_TAXONOMY_CODE_3), '') AS tertiary_taxonomy_code,
        
        -- Gender (for personalization)
        NULLIF(TRIM(PROVIDER_SEX_CODE), '') AS gender_code,
        
        -- Authorized Official (for organizations - decision maker)
        NULLIF(TRIM(AUTHORIZED_OFFICIAL_FIRST_NAME), '') AS auth_official_first_name,
        NULLIF(TRIM(AUTHORIZED_OFFICIAL_LAST_NAME), '') AS auth_official_last_name,
        NULLIF(TRIM(AUTHORIZED_OFFICIAL_TELEPHONE_NUMBER), '') AS auth_official_phone,
        NULLIF(TRIM(AUTHORIZED_OFFICIAL_CREDENTIAL_TEXT), '') AS auth_official_credential,
        
        -- Dates
        NULLIF(TRIM(PROVIDER_ENUMERATION_DATE), '') AS enumeration_date,
        NULLIF(TRIM(LAST_UPDATE_DATE), '') AS last_update_date,
        
        -- License info (first license for state verification)
        NULLIF(TRIM(PROVIDER_LICENSE_NUMBER_1), '') AS license_number,
        NULLIF(TRIM(PROVIDER_LICENSE_NUMBER_STATE_CODE_1), '') AS license_state,
        
        -- Sole Proprietor flag
        NULLIF(TRIM(IS_SOLE_PROPRIETOR), '') AS is_sole_proprietor
        
    FROM RAW.NPI_DATA
    WHERE 
        -- Dental taxonomy codes (all dental specialties start with 1223)
        HEALTHCARE_PROVIDER_TAXONOMY_CODE_1 LIKE '1223%'
        -- Active providers only
        AND (NPI_DEACTIVATION_DATE IS NULL OR TRIM(NPI_DEACTIVATION_DATE) = '')
        -- US-based practices only
        AND PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_COUNTRY_CODE = 'US'
)

SELECT
    -- Primary Key
    NPI,
    entity_type_code,
    entity_type,
    
    -- Name fields
    first_name,
    middle_name,
    last_name,
    name_prefix,
    name_suffix,
    credential,
    
    -- Cleaned full name (remove extra spaces)
    REGEXP_REPLACE(TRIM(full_name_raw), '\\s+', ' ') AS provider_full_name,
    
    -- Display name (for emails, etc.)
    CASE 
        WHEN entity_type_code = '1' THEN 
            COALESCE(name_prefix, '') || 
            CASE WHEN name_prefix IS NOT NULL THEN ' ' ELSE '' END ||
            COALESCE(first_name, '') || ' ' || COALESCE(last_name, '') ||
            CASE WHEN credential IS NOT NULL THEN ', ' || credential ELSE '' END
        ELSE 
            COALESCE(organization_name, dba_name, 'Unknown Organization')
    END AS display_name,
    
    -- Organization
    organization_name,
    dba_name,
    
    -- Practice Address (cleaned)
    practice_address_1,
    practice_address_2,
    CONCAT_WS(', ', 
        practice_address_1, 
        practice_address_2
    ) AS practice_address_full,
    practice_city,
    practice_state,
    LEFT(practice_zip_raw, 5) AS practice_zip,  -- Normalize to 5-digit
    practice_zip_raw AS practice_zip_full,
    practice_country,
    
    -- Phone (cleaned - remove non-digits for validation)
    practice_phone_raw,
    REGEXP_REPLACE(practice_phone_raw, '[^0-9]', '') AS practice_phone_clean,
    practice_fax_raw,
    
    -- Mailing Address
    mailing_address_1,
    mailing_address_2,
    mailing_city,
    mailing_state,
    LEFT(mailing_zip_raw, 5) AS mailing_zip,
    
    -- Specialty
    primary_taxonomy_code,
    secondary_taxonomy_code,
    tertiary_taxonomy_code,
    
    -- Gender
    gender_code,
    CASE gender_code
        WHEN 'M' THEN 'Male'
        WHEN 'F' THEN 'Female'
        ELSE NULL
    END AS gender,
    
    -- Decision Maker (for organizations)
    auth_official_first_name,
    auth_official_last_name,
    CASE 
        WHEN auth_official_first_name IS NOT NULL AND auth_official_last_name IS NOT NULL 
        THEN auth_official_first_name || ' ' || auth_official_last_name
        ELSE NULL
    END AS auth_official_full_name,
    auth_official_phone,
    auth_official_credential,
    
    -- Dates
    TRY_TO_DATE(enumeration_date, 'MM/DD/YYYY') AS enumeration_date,
    TRY_TO_DATE(last_update_date, 'MM/DD/YYYY') AS npi_last_update_date,
    
    -- License
    license_number,
    license_state,
    
    -- Flags
    is_sole_proprietor,
    CASE WHEN is_sole_proprietor = 'Y' THEN TRUE ELSE FALSE END AS is_sole_proprietor_flag,
    
    -- Validation status (to be updated by validation pipeline)
    NULL::BOOLEAN AS address_validated,
    NULL::FLOAT AS address_confidence,
    NULL::BOOLEAN AS phone_validated,
    NULL::VARCHAR(20) AS phone_line_type,
    
    -- Enrichment status (to be updated by enrichment pipeline)
    NULL::VARCHAR(255) AS enriched_email,
    NULL::VARCHAR(255) AS enriched_linkedin_url,
    NULL::VARCHAR(100) AS enrichment_source,
    NULL::TIMESTAMP AS enriched_at,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at

FROM dental_base
-- Deduplicate on NPI (should be unique, but just in case)
QUALIFY ROW_NUMBER() OVER (PARTITION BY NPI ORDER BY last_update_date DESC NULLS LAST) = 1;

-- Add clustering for performance on common query patterns
ALTER TABLE CLEAN.DENTAL_PROVIDERS CLUSTER BY (practice_state, entity_type_code);

-- Create indexes via search optimization (Snowflake)
-- ALTER TABLE CLEAN.DENTAL_PROVIDERS ADD SEARCH OPTIMIZATION;

-- ============================================================
-- Verification Queries
-- ============================================================

-- Total count
SELECT 'Total Dental Providers' AS metric, COUNT(*) AS value FROM CLEAN.DENTAL_PROVIDERS
UNION ALL
SELECT 'Individual Dentists', COUNT(*) FROM CLEAN.DENTAL_PROVIDERS WHERE entity_type = 'Individual'
UNION ALL
SELECT 'Organizations', COUNT(*) FROM CLEAN.DENTAL_PROVIDERS WHERE entity_type = 'Organization';

-- By state (top 10)
SELECT 
    practice_state,
    COUNT(*) AS provider_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM CLEAN.DENTAL_PROVIDERS
GROUP BY practice_state
ORDER BY provider_count DESC
LIMIT 10;

-- By specialty
SELECT 
    t.specialty_name,
    COUNT(*) AS provider_count
FROM CLEAN.DENTAL_PROVIDERS p
LEFT JOIN CLEAN.DENTAL_TAXONOMY_CODES t ON p.primary_taxonomy_code = t.taxonomy_code
GROUP BY t.specialty_name
ORDER BY provider_count DESC;

-- Sample records
SELECT 
    NPI,
    display_name,
    entity_type,
    practice_address_full,
    practice_city,
    practice_state,
    practice_zip,
    practice_phone_clean
FROM CLEAN.DENTAL_PROVIDERS
WHERE entity_type = 'Individual'
LIMIT 5;

