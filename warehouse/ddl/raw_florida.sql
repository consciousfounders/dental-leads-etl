-- ============================================================================
-- RAW FLORIDA DENTAL LICENSE DATA - SNOWFLAKE DDL
-- ============================================================================
-- Data source: FL DOH MQA (https://mqa-internet.doh.state.fl.us/MQASearchServices/)
-- ============================================================================

USE DATABASE DENTAL_DATA;
CREATE SCHEMA IF NOT EXISTS RAW_FLORIDA;
USE SCHEMA RAW_FLORIDA;

-- ============================================================================
-- DENTAL LICENSES TABLE
-- ============================================================================
CREATE OR REPLACE TABLE RAW_FLORIDA.DENTAL_LICENSES (
    STATE_CODE VARCHAR(2) DEFAULT 'FL',
    PROFESSIONAL_TYPE VARCHAR(50),
    LICENSE_TYPE VARCHAR(100),
    LICENSE_NUMBER VARCHAR(50),
    STATUS VARCHAR(50),
    STATUS_CATEGORY VARCHAR(20),
    FIRST_NAME VARCHAR(100),
    MIDDLE_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    FULL_NAME VARCHAR(200),
    ADDRESS VARCHAR(300),
    CITY VARCHAR(100),
    STATE VARCHAR(10),
    ZIP_CODE VARCHAR(20),
    COUNTY VARCHAR(100),
    ORIGINAL_ISSUE_DATE VARCHAR(20),
    EXPIRATION_DATE VARCHAR(20),
    LAST_RENEWAL_DATE VARCHAR(20),
    BOARD VARCHAR(100),
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE VARCHAR(255)
);

-- ============================================================================
-- FILE FORMAT
-- ============================================================================
CREATE OR REPLACE FILE FORMAT RAW_FLORIDA.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null')
    TRIM_SPACE = TRUE;

-- ============================================================================
-- STAGE
-- ============================================================================
CREATE OR REPLACE STAGE RAW_FLORIDA.FL_LICENSE_STAGE
    FILE_FORMAT = RAW_FLORIDA.CSV_FORMAT;

-- ============================================================================
-- LOAD COMMAND
-- ============================================================================
/*
COPY INTO RAW_FLORIDA.DENTAL_LICENSES (
    STATE_CODE, PROFESSIONAL_TYPE, LICENSE_TYPE, LICENSE_NUMBER,
    STATUS, STATUS_CATEGORY, FIRST_NAME, MIDDLE_NAME, LAST_NAME, FULL_NAME,
    ADDRESS, CITY, STATE, ZIP_CODE, COUNTY,
    ORIGINAL_ISSUE_DATE, EXPIRATION_DATE, LAST_RENEWAL_DATE, BOARD
)
FROM @RAW_FLORIDA.FL_LICENSE_STAGE/fl_dental_all_
FILE_FORMAT = RAW_FLORIDA.CSV_FORMAT
PATTERN = '.*fl_dental_all_.*\.csv'
ON_ERROR = 'CONTINUE';
*/

-- ============================================================================
-- ALTERNATIVE: Load from MQA Data Download Portal (Bulk Files)
-- ============================================================================
-- If you register at https://data-download.mqa.flhealthsource.gov/
-- you can download pipe-delimited bulk files with more complete data.
-- Those files have different schema - adjust accordingly.

/*
-- Example for bulk download file (pipe-delimited)
CREATE OR REPLACE FILE FORMAT RAW_FLORIDA.PIPE_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null')
    TRIM_SPACE = TRUE;
*/
