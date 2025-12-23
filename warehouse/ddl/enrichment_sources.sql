-- ============================================================================
-- ENRICHMENT SOURCES - SNOWFLAKE DDL
-- ============================================================================
-- Tables for storing enrichment data from Apollo, Wiza, and custom scrapers
-- ============================================================================

USE DATABASE DENTAL_DATA;
CREATE SCHEMA IF NOT EXISTS RAW_ENRICHMENTS;
USE SCHEMA RAW_ENRICHMENTS;

-- ============================================================================
-- 1. APOLLO ENRICHMENTS (B2B Data)
-- ============================================================================
-- Data from Apollo.io API or exports
-- Primary source for: email, company info, title

CREATE OR REPLACE TABLE RAW_ENRICHMENTS.APOLLO_ENRICHMENTS (
    -- Matching keys
    PROVIDER_ID VARCHAR(50),              -- Our internal ID
    NPI VARCHAR(10),                      -- NPI if available
    LICENSE_NUMBER VARCHAR(50),
    LICENSE_STATE VARCHAR(2),

    -- Apollo IDs
    APOLLO_ID VARCHAR(50),
    APOLLO_CONTACT_ID VARCHAR(50),
    APOLLO_ORGANIZATION_ID VARCHAR(50),

    -- Contact info
    EMAIL VARCHAR(255),
    EMAIL_CONFIDENCE VARCHAR(20),         -- 'verified', 'likely', 'guess'
    PHONE VARCHAR(30),
    LINKEDIN_URL VARCHAR(500),

    -- Company info
    COMPANY_NAME VARCHAR(255),
    COMPANY_DOMAIN VARCHAR(255),
    WEBSITE_URL VARCHAR(500),
    INDUSTRY VARCHAR(100),
    EMPLOYEE_COUNT INT,
    ANNUAL_REVENUE VARCHAR(50),

    -- Title/role
    TITLE VARCHAR(200),
    SENIORITY VARCHAR(50),
    DEPARTMENTS VARIANT,                  -- JSON array

    -- Location
    CITY VARCHAR(100),
    STATE VARCHAR(50),
    COUNTRY VARCHAR(50),

    -- Metadata
    ENRICHED_AT TIMESTAMP_NTZ,
    API_CREDITS_USED INT,
    _LOAD_ID VARCHAR(36),
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE INDEX IDX_APOLLO_PROVIDER ON RAW_ENRICHMENTS.APOLLO_ENRICHMENTS (PROVIDER_ID);
CREATE OR REPLACE INDEX IDX_APOLLO_EMAIL ON RAW_ENRICHMENTS.APOLLO_ENRICHMENTS (EMAIL);

-- ============================================================================
-- 2. WIZA ENRICHMENTS (LinkedIn-based)
-- ============================================================================
-- Data from Wiza exports (LinkedIn email finder)
-- Primary source for: LinkedIn URL, work email

CREATE OR REPLACE TABLE RAW_ENRICHMENTS.WIZA_ENRICHMENTS (
    -- Matching keys
    PROVIDER_ID VARCHAR(50),
    NPI VARCHAR(10),
    LICENSE_NUMBER VARCHAR(50),
    LICENSE_STATE VARCHAR(2),

    -- Wiza IDs
    WIZA_ID VARCHAR(50),

    -- Contact info
    EMAIL VARCHAR(255),
    EMAIL_TYPE VARCHAR(20),               -- 'work', 'personal'
    EMAIL_STATUS VARCHAR(20),             -- 'valid', 'catch-all', 'unknown'
    PHONE VARCHAR(30),

    -- LinkedIn
    LINKEDIN_URL VARCHAR(500),
    LINKEDIN_HEADLINE VARCHAR(500),
    LINKEDIN_CONNECTIONS INT,

    -- Company (from LinkedIn)
    COMPANY_NAME VARCHAR(255),
    COMPANY_LINKEDIN_URL VARCHAR(500),
    COMPANY_WEBSITE VARCHAR(500),

    -- Title
    TITLE VARCHAR(200),

    -- Location
    LOCATION VARCHAR(200),
    CITY VARCHAR(100),
    STATE VARCHAR(50),

    -- Metadata
    ENRICHED_AT TIMESTAMP_NTZ,
    _LOAD_ID VARCHAR(36),
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE INDEX IDX_WIZA_PROVIDER ON RAW_ENRICHMENTS.WIZA_ENRICHMENTS (PROVIDER_ID);
CREATE OR REPLACE INDEX IDX_WIZA_LINKEDIN ON RAW_ENRICHMENTS.WIZA_ENRICHMENTS (LINKEDIN_URL);

-- ============================================================================
-- 3. SCRAPED DATA (Custom scrapers)
-- ============================================================================
-- Data from custom web scrapers (practice websites, directories, reviews)
-- Primary source for: practice website, hours, specialties, reviews

CREATE OR REPLACE TABLE RAW_ENRICHMENTS.SCRAPED_DATA (
    -- Matching keys
    PROVIDER_ID VARCHAR(50),
    NPI VARCHAR(10),
    LICENSE_NUMBER VARCHAR(50),
    LICENSE_STATE VARCHAR(2),

    -- Scrape source
    SCRAPE_SOURCE VARCHAR(50),            -- 'google_maps', 'yelp', 'healthgrades', 'practice_website'
    SOURCE_URL VARCHAR(1000),

    -- Contact info (scraped)
    EMAIL VARCHAR(255),
    PHONE VARCHAR(30),
    WEBSITE VARCHAR(500),

    -- Practice info
    PRACTICE_NAME VARCHAR(255),
    PRACTICE_ADDRESS VARCHAR(500),
    PRACTICE_CITY VARCHAR(100),
    PRACTICE_STATE VARCHAR(50),
    PRACTICE_ZIP VARCHAR(20),
    PRACTICE_HOURS VARIANT,               -- JSON: {"monday": "9-5", ...}

    -- Reviews/ratings
    RATING_SCORE FLOAT,                   -- e.g., 4.5
    RATING_COUNT INT,                     -- Number of reviews
    REVIEWS_SUMMARY TEXT,                 -- AI-generated summary of reviews

    -- Specialties (scraped)
    SPECIALTIES VARIANT,                  -- JSON array
    SERVICES VARIANT,                     -- JSON array
    INSURANCE_ACCEPTED VARIANT,           -- JSON array

    -- Social
    FACEBOOK_URL VARCHAR(500),
    INSTAGRAM_URL VARCHAR(500),
    TWITTER_URL VARCHAR(500),

    -- Metadata
    SCRAPED_AT TIMESTAMP_NTZ,
    SCRAPE_SUCCESS BOOLEAN,
    SCRAPE_ERROR TEXT,
    _LOAD_ID VARCHAR(36),
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE INDEX IDX_SCRAPED_PROVIDER ON RAW_ENRICHMENTS.SCRAPED_DATA (PROVIDER_ID);
CREATE OR REPLACE INDEX IDX_SCRAPED_SOURCE ON RAW_ENRICHMENTS.SCRAPED_DATA (SCRAPE_SOURCE);

-- ============================================================================
-- 4. ENRICHMENT QUEUE (pending enrichments)
-- ============================================================================
-- Track what needs to be enriched and status

CREATE OR REPLACE TABLE RAW_ENRICHMENTS.ENRICHMENT_QUEUE (
    QUEUE_ID VARCHAR(36) PRIMARY KEY,
    PROVIDER_ID VARCHAR(50) NOT NULL,

    -- What to enrich
    FIRST_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    COMPANY_NAME VARCHAR(255),
    CITY VARCHAR(100),
    STATE VARCHAR(50),
    LINKEDIN_URL VARCHAR(500),            -- If known (for Wiza)

    -- Enrichment source
    SOURCE VARCHAR(20) NOT NULL,          -- 'apollo', 'wiza', 'scraper'

    -- Status
    STATUS VARCHAR(20) DEFAULT 'pending', -- 'pending', 'processing', 'completed', 'failed', 'skipped'
    PRIORITY INT DEFAULT 50,              -- 1-100, higher = more important

    -- Scheduling
    QUEUED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PROCESS_AFTER TIMESTAMP_NTZ,          -- Don't process before this time
    PROCESSED_AT TIMESTAMP_NTZ,

    -- Results
    RESULT_ID VARCHAR(50),                -- ID in enrichment table
    ERROR_MESSAGE TEXT,
    CREDITS_USED INT,

    -- Rate limiting
    ATTEMPT_COUNT INT DEFAULT 0,
    LAST_ATTEMPT_AT TIMESTAMP_NTZ,
    NEXT_RETRY_AT TIMESTAMP_NTZ,

    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- 5. ENRICHMENT COSTS TRACKING
-- ============================================================================
-- Track costs per enrichment source for budgeting

CREATE OR REPLACE TABLE RAW_ENRICHMENTS.ENRICHMENT_COSTS (
    COST_ID VARCHAR(36) PRIMARY KEY,
    DATE DATE NOT NULL,
    SOURCE VARCHAR(20) NOT NULL,          -- 'apollo', 'wiza', 'scraper'

    -- Usage
    CREDITS_USED INT,
    RECORDS_ENRICHED INT,
    RECORDS_FOUND INT,                    -- With usable data
    SUCCESS_RATE FLOAT,

    -- Costs
    COST_PER_CREDIT FLOAT,
    TOTAL_COST_USD FLOAT,
    COST_PER_ENRICHED_RECORD FLOAT,

    -- Quality metrics
    EMAIL_FOUND_PCT FLOAT,
    PHONE_FOUND_PCT FLOAT,
    EMAIL_VALID_PCT FLOAT,                -- If verified

    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- 6. HELPFUL VIEWS
-- ============================================================================

-- Providers needing enrichment (have license but no email)
CREATE OR REPLACE VIEW RAW_ENRICHMENTS.V_NEEDS_ENRICHMENT AS
SELECT
    g.provider_id,
    g.npi_number,
    g.license_state,
    g.license_number,
    g.first_name,
    g.last_name,
    g.company_name,
    g.city,
    g.state,
    g.is_new_licensee,
    g.days_since_licensed,
    g.enrichment_score,
    CASE
        WHEN g.is_new_licensee AND g.days_since_licensed <= 30 THEN 100  -- Hot lead
        WHEN g.is_new_licensee THEN 80
        WHEN g.days_since_licensed <= 365 THEN 50
        ELSE 30
    END AS enrichment_priority
FROM INTEGRATION.INT_PROVIDER_GOLDEN g
WHERE g.email IS NULL
  AND g.license_status_code IN (20, 46, 70)  -- Active
ORDER BY enrichment_priority DESC;

-- Enrichment coverage by state
CREATE OR REPLACE VIEW RAW_ENRICHMENTS.V_ENRICHMENT_COVERAGE AS
SELECT
    license_state,
    professional_type,
    COUNT(*) AS total_providers,
    SUM(CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END) AS has_email,
    SUM(CASE WHEN phone IS NOT NULL THEN 1 ELSE 0 END) AS has_phone,
    SUM(CASE WHEN linkedin_url IS NOT NULL THEN 1 ELSE 0 END) AS has_linkedin,
    SUM(CASE WHEN website IS NOT NULL THEN 1 ELSE 0 END) AS has_website,
    ROUND(SUM(CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS email_pct,
    ROUND(SUM(CASE WHEN phone IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS phone_pct,
    AVG(enrichment_score) AS avg_enrichment_score
FROM INTEGRATION.INT_PROVIDER_GOLDEN
WHERE license_status_code IN (20, 46, 70)
GROUP BY 1, 2
ORDER BY 1, 2;

-- Daily enrichment summary
CREATE OR REPLACE VIEW RAW_ENRICHMENTS.V_ENRICHMENT_DAILY AS
SELECT
    DATE(_LOADED_AT) AS load_date,
    'apollo' AS source,
    COUNT(*) AS records_loaded,
    SUM(CASE WHEN EMAIL IS NOT NULL THEN 1 ELSE 0 END) AS emails_found
FROM RAW_ENRICHMENTS.APOLLO_ENRICHMENTS
GROUP BY 1
UNION ALL
SELECT
    DATE(_LOADED_AT) AS load_date,
    'wiza' AS source,
    COUNT(*) AS records_loaded,
    SUM(CASE WHEN EMAIL IS NOT NULL THEN 1 ELSE 0 END) AS emails_found
FROM RAW_ENRICHMENTS.WIZA_ENRICHMENTS
GROUP BY 1
UNION ALL
SELECT
    DATE(_LOADED_AT) AS load_date,
    'scraped' AS source,
    COUNT(*) AS records_loaded,
    SUM(CASE WHEN EMAIL IS NOT NULL THEN 1 ELSE 0 END) AS emails_found
FROM RAW_ENRICHMENTS.SCRAPED_DATA
GROUP BY 1
ORDER BY 1 DESC, 2;
