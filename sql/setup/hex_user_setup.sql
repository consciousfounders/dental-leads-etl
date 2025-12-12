-- ============================================================================
-- Hex User Setup for Snowflake
-- ============================================================================
-- Run this script as ACCOUNTADMIN to create a dedicated user for Hex
-- with read-only access to the DENTAL_LEADS database.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- 1. Create Role
-- ============================================================================
CREATE ROLE IF NOT EXISTS HEX_ROLE
  COMMENT = 'Role for Hex data platform with read-only access';

-- ============================================================================
-- 2. Grant Warehouse Access
-- ============================================================================
GRANT USAGE ON WAREHOUSE DL_WH TO ROLE HEX_ROLE;

-- ============================================================================
-- 3. Grant Database Access
-- ============================================================================
GRANT USAGE ON DATABASE DENTAL_LEADS TO ROLE HEX_ROLE;

-- Grant access to all current and future schemas
GRANT USAGE ON ALL SCHEMAS IN DATABASE DENTAL_LEADS TO ROLE HEX_ROLE;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE DENTAL_LEADS TO ROLE HEX_ROLE;

-- ============================================================================
-- 4. Grant Table/View Access (Read-Only)
-- ============================================================================
-- Current tables
GRANT SELECT ON ALL TABLES IN DATABASE DENTAL_LEADS TO ROLE HEX_ROLE;
GRANT SELECT ON ALL VIEWS IN DATABASE DENTAL_LEADS TO ROLE HEX_ROLE;

-- Future tables (auto-grant for new tables)
GRANT SELECT ON FUTURE TABLES IN DATABASE DENTAL_LEADS TO ROLE HEX_ROLE;
GRANT SELECT ON FUTURE VIEWS IN DATABASE DENTAL_LEADS TO ROLE HEX_ROLE;

-- ============================================================================
-- 5. Create User
-- ============================================================================
CREATE USER IF NOT EXISTS HEX_USER
  DEFAULT_ROLE = HEX_ROLE
  DEFAULT_WAREHOUSE = DL_WH
  DEFAULT_NAMESPACE = DENTAL_LEADS.RAW
  MUST_CHANGE_PASSWORD = FALSE
  COMMENT = 'Service account for Hex data platform';

-- Assign role to user
GRANT ROLE HEX_ROLE TO USER HEX_USER;

-- ============================================================================
-- 6. Set Authentication
-- ============================================================================
-- OPTION A: Password Authentication (simpler, less secure)
-- Uncomment and set a strong password:
-- ALTER USER HEX_USER SET PASSWORD = 'YourSecurePassword123!';

-- OPTION B: RSA Key-Pair Authentication (recommended for production)
-- Generate key pair using:
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out hex_rsa_key.p8 -nocrypt
--   openssl rsa -in hex_rsa_key.p8 -pubout -out hex_rsa_key.pub
-- Then uncomment and paste the public key (without headers/newlines):
-- ALTER USER HEX_USER SET RSA_PUBLIC_KEY = 'MIIBIjANBgkq...';

-- ============================================================================
-- 7. Verification
-- ============================================================================
-- Test the grants:
SHOW GRANTS TO ROLE HEX_ROLE;
SHOW GRANTS TO USER HEX_USER;

-- Describe the user:
DESCRIBE USER HEX_USER;

-- ============================================================================
-- Connection Details for Hex:
-- ============================================================================
-- Account:    <your-account>.<region> (e.g., abc12345.us-east-1)
-- User:       HEX_USER
-- Warehouse:  DL_WH
-- Database:   DENTAL_LEADS
-- Schema:     RAW
-- Role:       HEX_ROLE
-- Auth:       Password or Key-Pair (based on option chosen above)
-- ============================================================================


