-- ============================================================
-- BigQuery: RAW → CLEAN: Dental Providers Transformation
-- ============================================================

CREATE OR REPLACE TABLE `silicon-will-480022-f8.dental_leads_clean.dental_providers`
CLUSTER BY practice_state, entity_type
AS
WITH dental_base AS (
    SELECT
        CAST(NPI AS STRING) AS NPI,
        CAST(Entity_Type_Code AS STRING) AS entity_type_code,
        CASE Entity_Type_Code
            WHEN 1 THEN 'Individual'
            WHEN 2 THEN 'Organization'
            ELSE 'Unknown'
        END AS entity_type,
        
        -- Provider Name
        NULLIF(TRIM(CAST(Provider_First_Name AS STRING)), '') AS first_name,
        NULLIF(TRIM(CAST(Provider_Middle_Name AS STRING)), '') AS middle_name,
        NULLIF(TRIM(CAST(Provider_Last_Name__Legal_Name_ AS STRING)), '') AS last_name,
        NULLIF(TRIM(CAST(Provider_Name_Prefix_Text AS STRING)), '') AS name_prefix,
        NULLIF(TRIM(CAST(Provider_Name_Suffix_Text AS STRING)), '') AS name_suffix,
        NULLIF(TRIM(CAST(Provider_Credential_Text AS STRING)), '') AS credential,
        
        -- Organization Name
        NULLIF(TRIM(CAST(Provider_Organization_Name__Legal_Business_Name_ AS STRING)), '') AS organization_name,
        NULLIF(TRIM(CAST(Provider_Other_Organization_Name AS STRING)), '') AS dba_name,
        
        -- Practice Location
        NULLIF(TRIM(CAST(Provider_First_Line_Business_Practice_Location_Address AS STRING)), '') AS practice_address_1,
        NULLIF(TRIM(CAST(Provider_Second_Line_Business_Practice_Location_Address AS STRING)), '') AS practice_address_2,
        NULLIF(TRIM(CAST(Provider_Business_Practice_Location_Address_City_Name AS STRING)), '') AS practice_city,
        NULLIF(TRIM(CAST(Provider_Business_Practice_Location_Address_State_Name AS STRING)), '') AS practice_state,
        NULLIF(TRIM(CAST(Provider_Business_Practice_Location_Address_Postal_Code AS STRING)), '') AS practice_zip_raw,
        NULLIF(TRIM(CAST(Provider_Business_Practice_Location_Address_Country_Code__If_outside_U_S__ AS STRING)), '') AS practice_country,
        NULLIF(TRIM(CAST(Provider_Business_Practice_Location_Address_Telephone_Number AS STRING)), '') AS practice_phone_raw,
        NULLIF(TRIM(CAST(Provider_Business_Practice_Location_Address_Fax_Number AS STRING)), '') AS practice_fax_raw,
        
        -- Mailing Address
        NULLIF(TRIM(CAST(Provider_First_Line_Business_Mailing_Address AS STRING)), '') AS mailing_address_1,
        NULLIF(TRIM(CAST(Provider_Second_Line_Business_Mailing_Address AS STRING)), '') AS mailing_address_2,
        NULLIF(TRIM(CAST(Provider_Business_Mailing_Address_City_Name AS STRING)), '') AS mailing_city,
        NULLIF(TRIM(CAST(Provider_Business_Mailing_Address_State_Name AS STRING)), '') AS mailing_state,
        NULLIF(TRIM(CAST(Provider_Business_Mailing_Address_Postal_Code AS STRING)), '') AS mailing_zip_raw,
        
        -- Taxonomy / Specialty
        NULLIF(TRIM(CAST(Healthcare_Provider_Taxonomy_Code_1 AS STRING)), '') AS primary_taxonomy_code,
        NULLIF(TRIM(CAST(Healthcare_Provider_Taxonomy_Code_2 AS STRING)), '') AS secondary_taxonomy_code,
        NULLIF(TRIM(CAST(Healthcare_Provider_Taxonomy_Code_3 AS STRING)), '') AS tertiary_taxonomy_code,
        
        -- Gender
        NULLIF(TRIM(CAST(Provider_Sex_Code AS STRING)), '') AS gender_code,
        
        -- Authorized Official
        NULLIF(TRIM(CAST(Authorized_Official_Last_Name AS STRING)), '') AS auth_official_last_name,
        NULLIF(TRIM(CAST(Authorized_Official_First_Name AS STRING)), '') AS auth_official_first_name,
        NULLIF(TRIM(CAST(Authorized_Official_Telephone_Number AS STRING)), '') AS auth_official_phone,
        NULLIF(TRIM(CAST(Authorized_Official_Title_or_Position AS STRING)), '') AS auth_official_credential,
        
        -- Dates
        CAST(Provider_Enumeration_Date AS STRING) AS enumeration_date_raw,
        CAST(Last_Update_Date AS STRING) AS last_update_date_raw,
        CAST(NPI_Deactivation_Date AS STRING) AS deactivation_date,
        
        -- License
        NULLIF(TRIM(CAST(Provider_License_Number_1 AS STRING)), '') AS license_number,
        NULLIF(TRIM(CAST(Provider_License_Number_State_Code_1 AS STRING)), '') AS license_state
        
    FROM `silicon-will-480022-f8.dental_leads.npi_raw`
    WHERE 
        -- Dental taxonomy codes
        CAST(Healthcare_Provider_Taxonomy_Code_1 AS STRING) LIKE '1223%'
        -- Active providers only
        AND (NPI_Deactivation_Date IS NULL OR TRIM(CAST(NPI_Deactivation_Date AS STRING)) = '')
        -- US-based practices
        AND CAST(Provider_Business_Practice_Location_Address_Country_Code__If_outside_U_S__ AS STRING) = 'US'
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY NPI ORDER BY last_update_date_raw DESC NULLS LAST) AS rn
    FROM dental_base
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
            CONCAT(
                COALESCE(CONCAT(name_prefix, ' '), ''),
                COALESCE(first_name, ''), ' ', COALESCE(last_name, ''),
                COALESCE(CONCAT(', ', credential), '')
            )
        ELSE 
            COALESCE(organization_name, dba_name, 'Unknown Organization')
    END AS display_name,
    
    organization_name,
    dba_name,
    
    practice_address_1,
    practice_address_2,
    CONCAT(
        COALESCE(practice_address_1, ''),
        CASE WHEN practice_address_2 IS NOT NULL THEN CONCAT(', ', practice_address_2) ELSE '' END
    ) AS practice_address_full,
    practice_city,
    practice_state,
    LEFT(practice_zip_raw, 5) AS practice_zip,
    practice_zip_raw AS practice_zip_full,
    practice_country,
    
    practice_phone_raw,
    REGEXP_REPLACE(practice_phone_raw, r'[^0-9]', '') AS practice_phone_clean,
    practice_fax_raw,
    
    mailing_address_1,
    mailing_address_2,
    mailing_city,
    mailing_state,
    LEFT(mailing_zip_raw, 5) AS mailing_zip,
    
    primary_taxonomy_code,
    secondary_taxonomy_code,
    tertiary_taxonomy_code,
    
    gender_code,
    CASE gender_code
        WHEN 'M' THEN 'Male'
        WHEN 'F' THEN 'Female'
        ELSE NULL
    END AS gender,
    
    auth_official_first_name,
    auth_official_last_name,
    CASE 
        WHEN auth_official_first_name IS NOT NULL AND auth_official_last_name IS NOT NULL 
        THEN CONCAT(auth_official_first_name, ' ', auth_official_last_name)
        ELSE NULL
    END AS auth_official_full_name,
    auth_official_phone,
    auth_official_credential,
    
    -- Parse dates
    SAFE.PARSE_DATE('%m/%d/%Y', enumeration_date_raw) AS enumeration_date,
    SAFE.PARSE_DATE('%m/%d/%Y', last_update_date_raw) AS npi_last_update_date,
    
    -- Practice Age Cohort
    CASE
        WHEN DATE_DIFF(CURRENT_DATE(), SAFE.PARSE_DATE('%m/%d/%Y', enumeration_date_raw), YEAR) <= 2 THEN 'Very New (0-2 yrs)'
        WHEN DATE_DIFF(CURRENT_DATE(), SAFE.PARSE_DATE('%m/%d/%Y', enumeration_date_raw), YEAR) <= 5 THEN 'New (2-5 yrs)'
        WHEN DATE_DIFF(CURRENT_DATE(), SAFE.PARSE_DATE('%m/%d/%Y', enumeration_date_raw), YEAR) <= 10 THEN 'Established (5-10 yrs)'
        WHEN DATE_DIFF(CURRENT_DATE(), SAFE.PARSE_DATE('%m/%d/%Y', enumeration_date_raw), YEAR) <= 20 THEN 'Mature (10-20 yrs)'
        ELSE 'Legacy (20+ yrs)'
    END AS practice_age_cohort,
    
    license_number,
    license_state,
    
    -- Enrichment placeholders
    CAST(NULL AS STRING) AS enriched_email,
    CAST(NULL AS STRING) AS enriched_linkedin_url,
    CAST(NULL AS STRING) AS enrichment_source,
    CAST(NULL AS TIMESTAMP) AS enriched_at,
    
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at

FROM ranked
WHERE rn = 1;
