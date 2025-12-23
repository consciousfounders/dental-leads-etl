-- ============================================================================
-- RAW COLORADO DENTAL LICENSE DATA - SNOWFLAKE DDL
-- ============================================================================
-- Data source: https://data.colorado.gov/Regulations/Professional-and-Occupational-Licenses-in-Colorado/7s5z-vewr
-- ============================================================================

USE DATABASE DENTAL_DATA;
CREATE SCHEMA IF NOT EXISTS RAW_COLORADO;
USE SCHEMA RAW_COLORADO;

-- ============================================================================
-- DENTAL LICENSES TABLE
-- ============================================================================
CREATE OR REPLACE TABLE RAW_COLORADO.DENTAL_LICENSES (
    STATE_CODE VARCHAR(2) DEFAULT 'CO',
    PROFESSIONAL_TYPE VARCHAR(50),
    LICENSE_TYPE_CODE VARCHAR(20),
    LICENSE_TYPE_DESCRIPTION VARCHAR(100),
    LICENSE_NUMBER VARCHAR(50),
    STATUS VARCHAR(50),
    FIRST_NAME VARCHAR(100),
    MIDDLE_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    CITY VARCHAR(100),
    STATE VARCHAR(10),
    ZIP_CODE VARCHAR(20),
    FIRST_ISSUE_DATE DATE,
    LAST_RENEWED_DATE DATE,
    EXPIRATION_DATE DATE,
    VERIFICATION_URL VARCHAR(500),
    HEALTHCARE_PROFILE_URL VARCHAR(500),
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE VARCHAR(255)
);

-- ============================================================================
-- FILE FORMAT
-- ============================================================================
CREATE OR REPLACE FILE FORMAT RAW_COLORADO.CSV_FORMAT
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
CREATE OR REPLACE STAGE RAW_COLORADO.CO_LICENSE_STAGE
    FILE_FORMAT = RAW_COLORADO.CSV_FORMAT;

-- ============================================================================
-- LOAD COMMAND
-- ============================================================================
/*
COPY INTO RAW_COLORADO.DENTAL_LICENSES (
    STATE_CODE, PROFESSIONAL_TYPE, LICENSE_TYPE_CODE, LICENSE_TYPE_DESCRIPTION,
    LICENSE_NUMBER, STATUS, FIRST_NAME, MIDDLE_NAME, LAST_NAME,
    CITY, STATE, ZIP_CODE, FIRST_ISSUE_DATE, LAST_RENEWED_DATE, EXPIRATION_DATE,
    VERIFICATION_URL, HEALTHCARE_PROFILE_URL
)
FROM @RAW_COLORADO.CO_LICENSE_STAGE/co_dental_all_
FILE_FORMAT = RAW_COLORADO.CSV_FORMAT
PATTERN = '.*co_dental_all_.*\.csv'
ON_ERROR = 'CONTINUE';
*/
