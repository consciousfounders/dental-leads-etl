-- ============================================================================
-- RAW TEXAS DENTAL LICENSE DATA - SNOWFLAKE DDL
-- ============================================================================
-- Run this to create the raw layer tables for Texas dental license data
-- Data source: https://tsbde.texas.gov/resources/licensee-lists/
-- ============================================================================

-- Create database and schemas if not exist
CREATE DATABASE IF NOT EXISTS DENTAL_DATA;
USE DATABASE DENTAL_DATA;

CREATE SCHEMA IF NOT EXISTS RAW_TEXAS;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS SNAPSHOTS;
CREATE SCHEMA IF NOT EXISTS MARTS;

USE SCHEMA RAW_TEXAS;

-- ============================================================================
-- DENTIST TABLE
-- ============================================================================
CREATE OR REPLACE TABLE RAW_TEXAS.DENTIST (
    REC_TYPE VARCHAR(50),
    LIC_ID INTEGER,
    LIC_NBR VARCHAR(20),
    LIC_STA_CDE INTEGER,
    LIC_STA_DESC VARCHAR(50),
    LIC_ORIG_DTE VARCHAR(20),
    LIC_EXPR_DTE VARCHAR(20),
    FIRST_NME VARCHAR(100),
    MIDDLE_NME VARCHAR(100),
    LAST_NME VARCHAR(100),
    FORMER_LAST_NME VARCHAR(100),
    GENDER VARCHAR(20),
    ADDRESS1 VARCHAR(200),
    ADDRESS2 VARCHAR(200),
    CITY VARCHAR(100),
    STATE VARCHAR(10),
    ZIP VARCHAR(20),
    COUNTY VARCHAR(100),
    COUNTRY VARCHAR(100),
    PHONE VARCHAR(30),
    NOX_PERMIT_DTE VARCHAR(20),
    LEVEL_1_DTE VARCHAR(20),
    LEVEL_2_DTE VARCHAR(20),
    LEVEL_3_DTE VARCHAR(20),
    LEVEL_4_DTE VARCHAR(20),
    PORTABILITY VARCHAR(10),
    DISC_ACTION VARCHAR(10),
    PRAC_DESC VARCHAR(50),
    PRAC_TYPES VARCHAR(20),
    GRAD_YR VARCHAR(10),
    SCHOOL VARCHAR(200),
    BIRTH_YEAR VARCHAR(10),
    SHRP_MOD VARCHAR(50),
    SPP_MOD VARCHAR(50),
    ERX_WAIVER VARCHAR(10),
    LEVEL_EXEMPT VARCHAR(10),
    ENTITY_NBR INTEGER,
    REMEDIAL_PLNS VARCHAR(10),
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE VARCHAR(255)
);

-- ============================================================================
-- HYGIENIST TABLE
-- ============================================================================
CREATE OR REPLACE TABLE RAW_TEXAS.HYGIENIST (
    REC_TYPE VARCHAR(50),
    LIC_ID INTEGER,
    LIC_NBR VARCHAR(20),
    LIC_STA_CDE INTEGER,
    LIC_STA_DESC VARCHAR(50),
    LIC_ORIG_DTE VARCHAR(20),
    LIC_EXPR_DTE VARCHAR(20),
    FIRST_NME VARCHAR(100),
    MIDDLE_NME VARCHAR(100),
    LAST_MNE VARCHAR(100),  -- NOTE: Source has typo, keeping as-is for compatibility
    FORMER_LAST_NME VARCHAR(100),
    GENDER VARCHAR(20),
    ADDRESS1 VARCHAR(200),
    ADDRESS2 VARCHAR(200),
    CITY VARCHAR(100),
    STATE VARCHAR(10),
    ZIP VARCHAR(20),
    COUNTY VARCHAR(100),
    COUNTRY VARCHAR(100),
    PHONE VARCHAR(30),
    SEALANT VARCHAR(10),
    DISC_ACTION VARCHAR(10),
    GRAD_YR VARCHAR(10),
    SCHOOL VARCHAR(200),
    BIRTH_YEAR VARCHAR(10),
    NOM_MOD VARCHAR(10),
    LIA_MOD VARCHAR(10),
    ENTITY_NBR INTEGER,
    REMEDIAL_PLNS VARCHAR(10),
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE VARCHAR(255)
);

-- ============================================================================
-- DENTAL ASSISTANT TABLE
-- ============================================================================
CREATE OR REPLACE TABLE RAW_TEXAS.DENTAL_ASSISTANT (
    REC_TYPE VARCHAR(50),
    LIC_ID INTEGER,
    LIC_NBR VARCHAR(20),
    LIC_STA_CDE INTEGER,
    LIC_STA_DESC VARCHAR(50),
    LIC_ORIG_DTE VARCHAR(20),
    LIC_EXPR_DTE VARCHAR(20),
    FIRST_NME VARCHAR(100),
    MIDDLE_NME VARCHAR(100),
    LAST_NME VARCHAR(100),
    GENDER VARCHAR(20),
    ADDRESS1 VARCHAR(200),
    ADDRESS2 VARCHAR(200),
    CITY VARCHAR(100),
    STATE VARCHAR(10),
    ZIP VARCHAR(20),
    COUNTY VARCHAR(100),
    COUNTRY VARCHAR(100),
    PHONE VARCHAR(30),
    DISC_ACTION VARCHAR(10),
    BIRTH_YEAR VARCHAR(10),
    NOM_MOD VARCHAR(10),
    ENTITY_NBR INTEGER,
    REMEDIAL_PLNS VARCHAR(10),
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE VARCHAR(255)
);

-- ============================================================================
-- LABS TABLE
-- ============================================================================
CREATE OR REPLACE TABLE RAW_TEXAS.LABS (
    REC_TYPE VARCHAR(50),
    LIC_ID INTEGER,
    LIC_NBR VARCHAR(20),
    LIC_STA_CDE INTEGER,
    LIC_STA_DESC VARCHAR(50),
    LIC_ORIG_DTE VARCHAR(20),
    LIC_EXPR_DTE VARCHAR(20),
    LAB_NME VARCHAR(200),
    ADDRESS1 VARCHAR(200),
    ADDRESS2 VARCHAR(200),
    CITY VARCHAR(100),
    STATE VARCHAR(10),
    ZIP VARCHAR(20),
    COUNTY VARCHAR(100),
    COUNTRY VARCHAR(100),
    PHONE VARCHAR(30),
    LAB_TYPE VARCHAR(50),
    DISC_ACTION VARCHAR(10),
    LAB_OWNER VARCHAR(200),
    LAB_MANAGER VARCHAR(200),
    LAB_CDT VARCHAR(200),
    ENTITY_NBR INTEGER,
    REMEDIAL_PLNS VARCHAR(10),
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE VARCHAR(255)
);

-- ============================================================================
-- ETN (Exception Tracking Numbers) TABLE
-- ============================================================================
CREATE OR REPLACE TABLE RAW_TEXAS.ETN (
    REC_TYPE VARCHAR(50),
    LIC_NBR VARCHAR(20),
    LIC_STA_CDE INTEGER,
    LIC_STA_DESC VARCHAR(50),
    RANK_EFCT_DTE VARCHAR(20),
    LIC_EXPR_DTE VARCHAR(20),
    FIRST_NME VARCHAR(100),
    MIDDLE_NME VARCHAR(100),
    LAST_NME VARCHAR(100),
    CITY VARCHAR(100),
    STATE VARCHAR(10),
    ZIP VARCHAR(20),
    COUNTY VARCHAR(100),
    COUNTRY VARCHAR(100),
    ERX_WAIVER VARCHAR(10),
    ENTITY_NBR INTEGER,
    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE VARCHAR(255)
);

-- ============================================================================
-- FILE FORMAT FOR CSV LOADING
-- ============================================================================
CREATE OR REPLACE FILE FORMAT RAW_TEXAS.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE_UNENCLOSED_FIELD = NONE
    NULL_IF = ('', 'NULL', 'null')
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- ============================================================================
-- STAGE FOR FILE UPLOAD (Internal Stage)
-- ============================================================================
CREATE OR REPLACE STAGE RAW_TEXAS.TEXAS_LICENSE_STAGE
    FILE_FORMAT = RAW_TEXAS.CSV_FORMAT;

-- ============================================================================
-- LOAD COMMANDS (Run after uploading files to stage)
-- ============================================================================
-- Step 1: Upload files to stage using SnowSQL or Snowflake UI
-- PUT file:///path/to/Dentist.csv @RAW_TEXAS.TEXAS_LICENSE_STAGE;
-- PUT file:///path/to/Hygienist.csv @RAW_TEXAS.TEXAS_LICENSE_STAGE;
-- etc.

-- Step 2: Copy into tables
/*
COPY INTO RAW_TEXAS.DENTIST
FROM @RAW_TEXAS.TEXAS_LICENSE_STAGE/Dentist.csv
FILE_FORMAT = RAW_TEXAS.CSV_FORMAT
ON_ERROR = 'CONTINUE';

COPY INTO RAW_TEXAS.HYGIENIST
FROM @RAW_TEXAS.TEXAS_LICENSE_STAGE/Hygienist.csv
FILE_FORMAT = RAW_TEXAS.CSV_FORMAT
ON_ERROR = 'CONTINUE';

COPY INTO RAW_TEXAS.DENTAL_ASSISTANT
FROM @RAW_TEXAS.TEXAS_LICENSE_STAGE/DentalAssistant.csv
FILE_FORMAT = RAW_TEXAS.CSV_FORMAT
ON_ERROR = 'CONTINUE';

COPY INTO RAW_TEXAS.LABS
FROM @RAW_TEXAS.TEXAS_LICENSE_STAGE/Labs.csv
FILE_FORMAT = RAW_TEXAS.CSV_FORMAT
ON_ERROR = 'CONTINUE';

COPY INTO RAW_TEXAS.ETN
FROM @RAW_TEXAS.TEXAS_LICENSE_STAGE/ETN.csv
FILE_FORMAT = RAW_TEXAS.CSV_FORMAT
ON_ERROR = 'CONTINUE';
*/

-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================
-- Run these after loading to verify data:
/*
SELECT 'DENTIST' as table_name, COUNT(*) as row_count FROM RAW_TEXAS.DENTIST
UNION ALL
SELECT 'HYGIENIST', COUNT(*) FROM RAW_TEXAS.HYGIENIST
UNION ALL
SELECT 'DENTAL_ASSISTANT', COUNT(*) FROM RAW_TEXAS.DENTAL_ASSISTANT
UNION ALL
SELECT 'LABS', COUNT(*) FROM RAW_TEXAS.LABS
UNION ALL
SELECT 'ETN', COUNT(*) FROM RAW_TEXAS.ETN;

-- Check status distribution
SELECT LIC_STA_DESC, COUNT(*) as cnt
FROM RAW_TEXAS.DENTIST
GROUP BY LIC_STA_DESC
ORDER BY cnt DESC;
*/
