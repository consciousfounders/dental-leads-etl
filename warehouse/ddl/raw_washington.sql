-- ============================================================================
-- RAW WASHINGTON DENTAL LICENSE DATA - SNOWFLAKE DDL
-- ============================================================================
-- Data source: https://data.wa.gov/health/Health-Care-Provider-Credential-Data/qxh8-f4bd
-- ============================================================================

USE DATABASE DENTAL_DATA;
CREATE SCHEMA IF NOT EXISTS RAW_WASHINGTON;
USE SCHEMA RAW_WASHINGTON;

-- ============================================================================
-- DENTAL CREDENTIALS TABLE (All types combined)
-- ============================================================================
CREATE OR REPLACE TABLE RAW_WASHINGTON.DENTAL_CREDENTIALS (
    STATE_CODE VARCHAR(2) DEFAULT 'WA',
    PROFESSIONAL_TYPE VARCHAR(50),
    CREDENTIAL_TYPE VARCHAR(100),
    LICENSE_NUMBER VARCHAR(50),
    STATUS VARCHAR(20),
    FIRST_NAME VARCHAR(100),
    MIDDLE_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    BIRTH_YEAR VARCHAR(10),
    FIRST_ISSUE_DATE DATE,
    LAST_ISSUE_DATE DATE,
    EXPIRATION_DATE DATE,
    CE_DUE_DATE DATE,
    HAS_ENFORCEMENT_ACTION BOOLEAN,
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE VARCHAR(255)
);

-- ============================================================================
-- FILE FORMAT
-- ============================================================================
CREATE OR REPLACE FILE FORMAT RAW_WASHINGTON.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null')
    TRIM_SPACE = TRUE
    DATE_FORMAT = 'YYYY-MM-DD';

-- ============================================================================
-- STAGE
-- ============================================================================
CREATE OR REPLACE STAGE RAW_WASHINGTON.WA_LICENSE_STAGE
    FILE_FORMAT = RAW_WASHINGTON.CSV_FORMAT;

-- ============================================================================
-- LOAD COMMAND
-- ============================================================================
/*
COPY INTO RAW_WASHINGTON.DENTAL_CREDENTIALS (
    STATE_CODE, PROFESSIONAL_TYPE, CREDENTIAL_TYPE, LICENSE_NUMBER, STATUS,
    FIRST_NAME, MIDDLE_NAME, LAST_NAME, BIRTH_YEAR,
    FIRST_ISSUE_DATE, LAST_ISSUE_DATE, EXPIRATION_DATE, CE_DUE_DATE,
    HAS_ENFORCEMENT_ACTION
)
FROM @RAW_WASHINGTON.WA_LICENSE_STAGE/wa_dental_all_
FILE_FORMAT = RAW_WASHINGTON.CSV_FORMAT
PATTERN = '.*wa_dental_all_.*\.csv'
ON_ERROR = 'CONTINUE';
*/
