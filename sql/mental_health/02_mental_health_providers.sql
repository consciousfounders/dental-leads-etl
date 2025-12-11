-- ============================================================
-- RAW → MENTAL_HEALTH: Provider Transformation
-- ============================================================
-- Filters 9.2M NPI records to ~1.4M mental health providers
-- ============================================================

CREATE OR REPLACE TABLE MENTAL_HEALTH.PROVIDERS AS
WITH mental_health_base AS (
    SELECT
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
        
        -- Organization Name
        NULLIF(TRIM(PROVIDER_ORGANIZATION_NAME), '') AS organization_name,
        NULLIF(TRIM(PROVIDER_OTHER_ORGANIZATION_NAME), '') AS dba_name,
        
        -- Practice Location
        NULLIF(TRIM(PROVIDER_FIRST_LINE_BUSINESS_PRACTICE_LOCATION_ADDRESS), '') AS practice_address_1,
        NULLIF(TRIM(PROVIDER_SECOND_LINE_BUSINESS_PRACTICE_LOCATION_ADDRESS), '') AS practice_address_2,
        NULLIF(TRIM(PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_CITY_NAME), '') AS practice_city,
        NULLIF(TRIM(PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_STATE_NAME), '') AS practice_state,
        NULLIF(TRIM(PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_POSTAL_CODE), '') AS practice_zip_raw,
        NULLIF(TRIM(PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_COUNTRY_CODE), '') AS practice_country,
        NULLIF(TRIM(PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_TELEPHONE_NUMBER), '') AS practice_phone_raw,
        NULLIF(TRIM(PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_FAX_NUMBER), '') AS practice_fax_raw,
        
        -- Taxonomy / Specialty
        NULLIF(TRIM(HEALTHCARE_PROVIDER_TAXONOMY_CODE_1), '') AS primary_taxonomy_code,
        NULLIF(TRIM(HEALTHCARE_PROVIDER_TAXONOMY_CODE_2), '') AS secondary_taxonomy_code,
        
        -- Gender
        NULLIF(TRIM(PROVIDER_SEX_CODE), '') AS gender_code,
        
        -- Authorized Official (for organizations)
        NULLIF(TRIM(AUTHORIZED_OFFICIAL_FIRST_NAME), '') AS auth_official_first_name,
        NULLIF(TRIM(AUTHORIZED_OFFICIAL_LAST_NAME), '') AS auth_official_last_name,
        NULLIF(TRIM(AUTHORIZED_OFFICIAL_TELEPHONE_NUMBER), '') AS auth_official_phone,
        NULLIF(TRIM(AUTHORIZED_OFFICIAL_CREDENTIAL_TEXT), '') AS auth_official_credential,
        
        -- Dates
        NULLIF(TRIM(PROVIDER_ENUMERATION_DATE), '') AS enumeration_date_raw,
        NULLIF(TRIM(LAST_UPDATE_DATE), '') AS last_update_date_raw,
        
        -- License
        NULLIF(TRIM(PROVIDER_LICENSE_NUMBER_1), '') AS license_number,
        NULLIF(TRIM(PROVIDER_LICENSE_NUMBER_STATE_CODE_1), '') AS license_state,
        
        -- Sole Proprietor
        NULLIF(TRIM(IS_SOLE_PROPRIETOR), '') AS is_sole_proprietor
        
    FROM RAW.NPI_DATA
    WHERE (
        -- Counselors
        HEALTHCARE_PROVIDER_TAXONOMY_CODE_1 LIKE '101Y%'
        -- Clinical Social Workers
        OR HEALTHCARE_PROVIDER_TAXONOMY_CODE_1 LIKE '1041C%'
        -- Marriage & Family Therapists
        OR HEALTHCARE_PROVIDER_TAXONOMY_CODE_1 LIKE '106H%'
        -- Psychologists
        OR HEALTHCARE_PROVIDER_TAXONOMY_CODE_1 LIKE '103T%'
        -- Psychiatrists
        OR HEALTHCARE_PROVIDER_TAXONOMY_CODE_1 LIKE '2084P%'
        -- Mental Health Facilities
        OR HEALTHCARE_PROVIDER_TAXONOMY_CODE_1 LIKE '261QM%'
        -- Rehabilitation Facilities
        OR HEALTHCARE_PROVIDER_TAXONOMY_CODE_1 LIKE '261QR%'
        -- Substance Abuse Facilities
        OR HEALTHCARE_PROVIDER_TAXONOMY_CODE_1 LIKE '324500%'
        -- Psychiatric Hospitals
        OR HEALTHCARE_PROVIDER_TAXONOMY_CODE_1 LIKE '283Q%'
    )
    -- Active providers only
    AND (NPI_DEACTIVATION_DATE IS NULL OR TRIM(NPI_DEACTIVATION_DATE) = '')
    -- US-based only
    AND PROVIDER_BUSINESS_PRACTICE_LOCATION_ADDRESS_COUNTRY_CODE = 'US'
)

SELECT
    NPI,
    entity_type_code,
    entity_type,
    
    first_name,
    middle_name,
    last_name,
    name_prefix,
    name_suffix,
    credential,
    
    -- Display name
    CASE 
        WHEN entity_type_code = '1' THEN 
            COALESCE(name_prefix, '') || 
            CASE WHEN name_prefix IS NOT NULL THEN ' ' ELSE '' END ||
            COALESCE(first_name, '') || ' ' || COALESCE(last_name, '') ||
            CASE WHEN credential IS NOT NULL THEN ', ' || credential ELSE '' END
        ELSE 
            COALESCE(organization_name, dba_name, 'Unknown Organization')
    END AS display_name,
    
    organization_name,
    dba_name,
    
    -- Practice Address
    practice_address_1,
    practice_address_2,
    CONCAT_WS(', ', practice_address_1, practice_address_2) AS practice_address_full,
    practice_city,
    practice_state,
    LEFT(practice_zip_raw, 5) AS practice_zip,
    practice_zip_raw AS practice_zip_full,
    practice_country,
    
    -- Phone
    practice_phone_raw,
    REGEXP_REPLACE(practice_phone_raw, '[^0-9]', '') AS practice_phone_clean,
    practice_fax_raw,
    
    -- Specialty
    primary_taxonomy_code,
    secondary_taxonomy_code,
    
    -- Join to taxonomy reference
    t.CATEGORY AS provider_category,
    t.SPECIALTY_NAME AS specialty_name,
    t.IS_FACILITY AS is_facility,
    
    -- Gender
    gender_code,
    CASE gender_code
        WHEN 'M' THEN 'Male'
        WHEN 'F' THEN 'Female'
        ELSE NULL
    END AS gender,
    
    -- Authorized Official
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
    TRY_TO_DATE(enumeration_date_raw, 'MM/DD/YYYY') AS enumeration_date,
    TRY_TO_DATE(last_update_date_raw, 'MM/DD/YYYY') AS npi_last_update_date,
    
    -- Practice Age Cohort
    CASE
        WHEN DATEDIFF('year', TRY_TO_DATE(enumeration_date_raw, 'MM/DD/YYYY'), CURRENT_DATE()) <= 2 THEN 'Very New (0-2 yrs)'
        WHEN DATEDIFF('year', TRY_TO_DATE(enumeration_date_raw, 'MM/DD/YYYY'), CURRENT_DATE()) <= 5 THEN 'New (2-5 yrs)'
        WHEN DATEDIFF('year', TRY_TO_DATE(enumeration_date_raw, 'MM/DD/YYYY'), CURRENT_DATE()) <= 10 THEN 'Established (5-10 yrs)'
        WHEN DATEDIFF('year', TRY_TO_DATE(enumeration_date_raw, 'MM/DD/YYYY'), CURRENT_DATE()) <= 20 THEN 'Mature (10-20 yrs)'
        ELSE 'Legacy (20+ yrs)'
    END AS practice_age_cohort,
    
    -- License
    license_number,
    license_state,
    
    -- Flags
    is_sole_proprietor,
    
    -- Enrichment placeholders
    NULL::VARCHAR(255) AS enriched_email,
    NULL::VARCHAR(255) AS enriched_linkedin_url,
    NULL::VARCHAR(100) AS enrichment_source,
    NULL::TIMESTAMP AS enriched_at,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at

FROM mental_health_base m
LEFT JOIN MENTAL_HEALTH.TAXONOMY_CODES t ON m.primary_taxonomy_code = t.TAXONOMY_CODE
-- Dedupe on NPI
QUALIFY ROW_NUMBER() OVER (PARTITION BY NPI ORDER BY last_update_date_raw DESC NULLS LAST) = 1;

-- Add clustering
ALTER TABLE MENTAL_HEALTH.PROVIDERS CLUSTER BY (practice_state, provider_category);

