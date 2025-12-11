-- ============================================================
-- BigQuery: CLEAN Layer Views for Analytics
-- ============================================================

-- US States filter (exclude territories)
-- AL, AK, AZ, AR, CA, CO, CT, DE, FL, GA, HI, ID, IL, IN, IA, KS, KY, LA, ME, MD, 
-- MA, MI, MN, MS, MO, MT, NE, NV, NH, NJ, NM, NY, NC, ND, OH, OK, OR, PA, RI, SC, 
-- SD, TN, TX, UT, VT, VA, WA, WV, WI, WY, DC

-- V_INDIVIDUAL_DENTISTS
CREATE OR REPLACE VIEW `silicon-will-480022-f8.dental_leads_clean.v_individual_dentists` AS
SELECT 
    NPI,
    display_name,
    first_name,
    last_name,
    credential,
    practice_city AS city,
    practice_state AS state,
    practice_zip AS zip,
    practice_phone_clean AS phone,
    primary_taxonomy_code AS taxonomy_code,
    gender,
    practice_age_cohort,
    enumeration_date,
    'Individual' AS record_type
FROM `silicon-will-480022-f8.dental_leads_clean.dental_providers`
WHERE entity_type = 'Individual'
AND practice_state IN ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC');

-- V_ORGANIZATIONS
CREATE OR REPLACE VIEW `silicon-will-480022-f8.dental_leads_clean.v_organizations` AS
SELECT 
    NPI,
    organization_name,
    dba_name,
    practice_address_full AS address,
    practice_city AS city,
    practice_state AS state,
    practice_zip AS zip,
    practice_phone_clean AS phone,
    primary_taxonomy_code AS taxonomy_code,
    auth_official_full_name,
    auth_official_credential,
    auth_official_phone,
    practice_age_cohort,
    'Organization' AS record_type
FROM `silicon-will-480022-f8.dental_leads_clean.dental_providers`
WHERE entity_type = 'Organization'
AND practice_state IN ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC');

-- V_STATE_INSIGHTS
CREATE OR REPLACE VIEW `silicon-will-480022-f8.dental_leads_segmented.v_state_insights` AS
SELECT
    practice_state AS state,
    COUNT(*) AS total_dentists,
    COUNTIF(gender = 'Female') AS female_dentists,
    ROUND(COUNTIF(gender = 'Female') * 100.0 / NULLIF(COUNT(*), 0), 1) AS female_pct,
    COUNTIF(practice_age_cohort IN ('Very New (0-2 yrs)', 'New (2-5 yrs)')) AS new_practices,
    COUNTIF(practice_age_cohort = 'Established (5-10 yrs)') AS growth_practices,
    COUNTIF(practice_age_cohort = 'Mature (10-20 yrs)') AS established_practices,
    ROUND(COUNTIF(practice_age_cohort IN ('Very New (0-2 yrs)', 'New (2-5 yrs)')) * 100.0 / NULLIF(COUNT(*), 0), 1) AS innovation_score,
    COUNT(DISTINCT practice_zip) AS zip_count,
    COUNT(DISTINCT practice_city) AS city_count
FROM `silicon-will-480022-f8.dental_leads_clean.dental_providers`
WHERE practice_state IN ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC')
GROUP BY 1
ORDER BY total_dentists DESC;

-- V_CITY_DENSITY
CREATE OR REPLACE VIEW `silicon-will-480022-f8.dental_leads_segmented.v_city_density` AS
SELECT
    practice_city AS city,
    practice_state AS state,
    COUNT(*) AS provider_count,
    COUNTIF(gender = 'Female') AS female_count,
    ROUND(COUNTIF(gender = 'Female') * 100.0 / NULLIF(COUNT(*), 0), 1) AS female_pct,
    COUNTIF(practice_age_cohort IN ('Very New (0-2 yrs)', 'New (2-5 yrs)')) AS new_practice_count,
    COUNTIF(practice_age_cohort = 'Established (5-10 yrs)') AS mid_practice_count,
    COUNTIF(practice_age_cohort = 'Mature (10-20 yrs)') AS established_count,
    ROUND(COUNTIF(practice_age_cohort IN ('Very New (0-2 yrs)', 'New (2-5 yrs)')) * 100.0 / NULLIF(COUNT(*), 0), 1) AS innovation_score
FROM `silicon-will-480022-f8.dental_leads_clean.dental_providers`
WHERE practice_state IN ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC')
GROUP BY 1, 2
ORDER BY provider_count DESC;

-- V_MARKET_OPPORTUNITY
CREATE OR REPLACE VIEW `silicon-will-480022-f8.dental_leads_segmented.v_market_opportunity` AS
WITH CityStats AS (
    SELECT
        practice_city AS city,
        practice_state AS state,
        COUNT(*) AS total_providers,
        COUNTIF(practice_age_cohort IN ('Very New (0-2 yrs)', 'New (2-5 yrs)')) AS new_practices,
        ROUND(COUNTIF(practice_age_cohort IN ('Very New (0-2 yrs)', 'New (2-5 yrs)')) * 100.0 / NULLIF(COUNT(*), 0), 1) AS new_practice_pct,
        ROUND(COUNTIF(gender = 'Female') * 100.0 / NULLIF(COUNT(*), 0), 1) AS female_pct
    FROM `silicon-will-480022-f8.dental_leads_clean.dental_providers`
    WHERE practice_state IN ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC')
    GROUP BY 1, 2
)
SELECT
    city,
    state,
    total_providers,
    new_practices,
    new_practice_pct,
    female_pct,
    CASE
        WHEN total_providers >= 50 AND new_practice_pct >= 15 THEN 'High Growth'
        WHEN total_providers >= 20 AND new_practice_pct >= 10 THEN 'Growing'
        WHEN total_providers >= 10 AND new_practice_pct >= 5 THEN 'Mid-Size'
        WHEN total_providers >= 5 THEN 'Established'
        ELSE 'Emerging'
    END AS market_type
FROM CityStats
ORDER BY new_practice_pct DESC, total_providers DESC;

-- V_PRACTICE_AGE_DISTRIBUTION
CREATE OR REPLACE VIEW `silicon-will-480022-f8.dental_leads_segmented.v_practice_age_distribution` AS
SELECT
    practice_age_cohort,
    COUNT(*) AS provider_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS percentage
FROM `silicon-will-480022-f8.dental_leads_clean.dental_providers`
WHERE practice_state IN ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC')
GROUP BY 1
ORDER BY 
    CASE practice_age_cohort
        WHEN 'Very New (0-2 yrs)' THEN 1
        WHEN 'New (2-5 yrs)' THEN 2
        WHEN 'Established (5-10 yrs)' THEN 3
        WHEN 'Mature (10-20 yrs)' THEN 4
        WHEN 'Legacy (20+ yrs)' THEN 5
    END;

