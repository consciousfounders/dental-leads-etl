-- ============================================================
-- Looker Service Account Setup for Dental Leads
-- ============================================================
-- Run this as ACCOUNTADMIN to create a read-only service account
-- for Looker to connect to Snowflake
-- ============================================================

-- 1. Create a role for Looker
CREATE ROLE IF NOT EXISTS LOOKER_ROLE;

-- 2. Grant usage on warehouse
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE LOOKER_ROLE;

-- 3. Grant usage on database
GRANT USAGE ON DATABASE DENTAL_LEADS TO ROLE LOOKER_ROLE;

-- 4. Grant usage on schemas
GRANT USAGE ON SCHEMA DENTAL_LEADS.RAW TO ROLE LOOKER_ROLE;
GRANT USAGE ON SCHEMA DENTAL_LEADS.CLEAN TO ROLE LOOKER_ROLE;
GRANT USAGE ON SCHEMA DENTAL_LEADS.ENRICHED TO ROLE LOOKER_ROLE;

-- 5. Grant SELECT on all current tables
GRANT SELECT ON ALL TABLES IN SCHEMA DENTAL_LEADS.RAW TO ROLE LOOKER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA DENTAL_LEADS.CLEAN TO ROLE LOOKER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA DENTAL_LEADS.ENRICHED TO ROLE LOOKER_ROLE;

-- 6. Grant SELECT on future tables (auto-grant for new tables)
GRANT SELECT ON FUTURE TABLES IN SCHEMA DENTAL_LEADS.RAW TO ROLE LOOKER_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DENTAL_LEADS.CLEAN TO ROLE LOOKER_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DENTAL_LEADS.ENRICHED TO ROLE LOOKER_ROLE;

-- 7. Grant SELECT on views
GRANT SELECT ON ALL VIEWS IN SCHEMA DENTAL_LEADS.CLEAN TO ROLE LOOKER_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA DENTAL_LEADS.CLEAN TO ROLE LOOKER_ROLE;

-- 8. Create service account user for Looker
CREATE USER IF NOT EXISTS LOOKER_USER
    PASSWORD = 'CHANGE_ME_IMMEDIATELY'  -- Change this!
    DEFAULT_ROLE = LOOKER_ROLE
    DEFAULT_WAREHOUSE = COMPUTE_WH
    DEFAULT_NAMESPACE = DENTAL_LEADS.CLEAN
    MUST_CHANGE_PASSWORD = TRUE;

-- 9. Grant role to user
GRANT ROLE LOOKER_ROLE TO USER LOOKER_USER;

-- ============================================================
-- Verification Queries
-- ============================================================

-- Check role grants
SHOW GRANTS TO ROLE LOOKER_ROLE;

-- Check user grants
SHOW GRANTS TO USER LOOKER_USER;

-- Test as Looker user (run separately)
-- USE ROLE LOOKER_ROLE;
-- SELECT COUNT(*) FROM DENTAL_LEADS.CLEAN.DENTAL_PROVIDERS;

