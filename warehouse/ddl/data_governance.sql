-- ============================================================================
-- DATA GOVERNANCE SCHEMA - SNOWFLAKE DDL
-- ============================================================================
-- Core tables for data hygiene, rollback capability, and export control
-- ============================================================================

USE DATABASE DENTAL_DATA;
CREATE SCHEMA IF NOT EXISTS GOVERNANCE;
USE SCHEMA GOVERNANCE;

-- ============================================================================
-- 1. DATA LOADS REGISTRY
-- ============================================================================
-- Tracks every data load for audit trail and rollback capability

CREATE OR REPLACE TABLE GOVERNANCE.DATA_LOADS (
    LOAD_ID VARCHAR(36) PRIMARY KEY,              -- UUID
    SOURCE_TYPE VARCHAR(50) NOT NULL,             -- 'tx_license', 'npi', 'wa_license', etc.
    SOURCE_STATE VARCHAR(2),                      -- State code if applicable
    SOURCE_FILE VARCHAR(500),                     -- Original filename
    SOURCE_URL VARCHAR(1000),                     -- Where data came from
    SOURCE_DATE DATE NOT NULL,                    -- Date of the source data

    -- Load metrics
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ROW_COUNT INT,
    ROW_COUNT_PREVIOUS INT,                       -- For delta comparison
    ROW_COUNT_DELTA_PCT FLOAT,                    -- % change from previous

    -- Lifecycle status
    STATUS VARCHAR(20) DEFAULT 'pending',         -- pending, validated, promoted, quarantined, archived

    -- Validation
    VALIDATED_AT TIMESTAMP_NTZ,
    VALIDATION_PASSED BOOLEAN,
    VALIDATION_ERRORS VARIANT,                    -- JSON array of error messages
    VALIDATION_WARNINGS VARIANT,                  -- JSON array of warnings

    -- Promotion (to marts)
    PROMOTED_AT TIMESTAMP_NTZ,
    PROMOTED_BY VARCHAR(100),                     -- 'auto' or username
    PROMOTION_DELAY_HOURS INT DEFAULT 24,         -- Hold period before auto-promote

    -- Quarantine
    QUARANTINED_AT TIMESTAMP_NTZ,
    QUARANTINED_BY VARCHAR(100),
    QUARANTINE_REASON TEXT,

    -- Rollback tracking
    ROLLED_BACK_AT TIMESTAMP_NTZ,
    ROLLBACK_REASON TEXT,
    EXPORTS_CANCELLED INT,                        -- Count of cancelled exports
    EXPORTS_REVERSED INT,                         -- Count of reversed exports

    -- Metadata
    NOTES TEXT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Index for common queries
CREATE OR REPLACE INDEX IDX_LOADS_SOURCE ON GOVERNANCE.DATA_LOADS (SOURCE_TYPE, SOURCE_DATE);
CREATE OR REPLACE INDEX IDX_LOADS_STATUS ON GOVERNANCE.DATA_LOADS (STATUS);

-- ============================================================================
-- 2. VALIDATION RULES
-- ============================================================================
-- Configurable validation rules per source type

CREATE OR REPLACE TABLE GOVERNANCE.VALIDATION_RULES (
    RULE_ID VARCHAR(36) PRIMARY KEY,
    SOURCE_TYPE VARCHAR(50) NOT NULL,             -- Which source this applies to
    RULE_NAME VARCHAR(100) NOT NULL,
    RULE_DESCRIPTION TEXT,

    -- Rule configuration
    RULE_TYPE VARCHAR(30) NOT NULL,               -- 'row_count', 'field_populated', 'value_distribution', 'custom_sql'
    RULE_CONFIG VARIANT NOT NULL,                 -- JSON config for the rule

    -- Severity
    SEVERITY VARCHAR(10) DEFAULT 'error',         -- 'error' (blocks), 'warning' (allows with flag)

    -- Status
    IS_ACTIVE BOOLEAN DEFAULT TRUE,

    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert default validation rules
INSERT INTO GOVERNANCE.VALIDATION_RULES (RULE_ID, SOURCE_TYPE, RULE_NAME, RULE_DESCRIPTION, RULE_TYPE, RULE_CONFIG, SEVERITY) VALUES
-- Row count rules
('rule-001', 'tx_license', 'row_count_min', 'Minimum row count sanity check', 'row_count', '{"min": 1000}', 'error'),
('rule-002', 'tx_license', 'row_count_delta', 'Row count change < 20%', 'row_count_delta', '{"max_delta_pct": 0.20}', 'error'),
('rule-003', 'tx_license', 'row_count_delta_warning', 'Row count change > 5% warning', 'row_count_delta', '{"max_delta_pct": 0.05}', 'warning'),

-- Field population rules
('rule-010', 'tx_license', 'license_number_required', 'License number must be populated', 'field_populated', '{"field": "LICENSE_NUMBER", "min_pct": 0.99}', 'error'),
('rule-011', 'tx_license', 'last_name_required', 'Last name must be populated', 'field_populated', '{"field": "LAST_NAME", "min_pct": 0.99}', 'error'),
('rule-012', 'tx_license', 'city_populated', 'City should be mostly populated', 'field_populated', '{"field": "CITY", "min_pct": 0.90}', 'warning'),

-- Distribution rules
('rule-020', 'tx_license', 'active_ratio', 'At least 50% should be active', 'value_distribution', '{"field": "STATUS_CODE", "value": 20, "min_pct": 0.50}', 'error'),

-- Date rules
('rule-030', 'tx_license', 'no_future_dates', 'No license dates in the future', 'date_range', '{"field": "LICENSE_ORIGINAL_DATE", "max": "CURRENT_DATE()"}', 'error'),
('rule-031', 'tx_license', 'no_ancient_dates', 'No license dates before 1900', 'date_range', '{"field": "LICENSE_ORIGINAL_DATE", "min": "1900-01-01"}', 'error'),

-- NPI rules
('rule-040', 'npi', 'row_count_min', 'Minimum NPI row count', 'row_count', '{"min": 100000}', 'error'),
('rule-041', 'npi', 'npi_format', 'NPI must be 10 digits', 'field_format', '{"field": "NPI", "pattern": "^[0-9]{10}$", "min_pct": 0.99}', 'error');

-- ============================================================================
-- 3. EXPORT QUEUE
-- ============================================================================
-- Staging table for all outbound data - nothing goes direct to external systems

CREATE OR REPLACE TABLE GOVERNANCE.EXPORT_QUEUE (
    EXPORT_ID VARCHAR(36) PRIMARY KEY,

    -- What to export
    PROVIDER_ID VARCHAR(50) NOT NULL,             -- Our internal provider ID
    DESTINATION VARCHAR(30) NOT NULL,             -- 'ghl', 'instantly', 'lob_postcard', 'lob_letter'
    PAYLOAD VARIANT NOT NULL,                     -- JSON payload to send

    -- Source tracking
    DATA_LOAD_ID VARCHAR(36),                     -- Which load this came from
    MATCH_CONFIDENCE INT,                         -- NPI match confidence (0-100)

    -- Approval workflow
    REQUIRES_APPROVAL BOOLEAN DEFAULT FALSE,
    AUTO_APPROVE_AFTER TIMESTAMP_NTZ,             -- Auto-approve at this time if not manual
    APPROVED_AT TIMESTAMP_NTZ,
    APPROVED_BY VARCHAR(100),                     -- 'auto' or username
    REJECTION_REASON TEXT,

    -- Sending
    QUEUED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SCHEDULED_SEND_AT TIMESTAMP_NTZ,              -- Don't send before this time
    SENT_AT TIMESTAMP_NTZ,

    -- Results
    EXTERNAL_ID VARCHAR(100),                     -- ID in destination system
    STATUS VARCHAR(20) DEFAULT 'queued',          -- queued, approved, scheduled, sent, failed, bounced, cancelled
    ERROR_MESSAGE TEXT,
    ERROR_CODE VARCHAR(50),

    -- Reversal (if supported)
    REVERSED_AT TIMESTAMP_NTZ,
    REVERSAL_REASON TEXT,

    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE INDEX IDX_EXPORT_STATUS ON GOVERNANCE.EXPORT_QUEUE (STATUS, DESTINATION);
CREATE OR REPLACE INDEX IDX_EXPORT_LOAD ON GOVERNANCE.EXPORT_QUEUE (DATA_LOAD_ID);
CREATE OR REPLACE INDEX IDX_EXPORT_PROVIDER ON GOVERNANCE.EXPORT_QUEUE (PROVIDER_ID);

-- ============================================================================
-- 4. EXPORT HISTORY (sent records - immutable audit log)
-- ============================================================================

CREATE OR REPLACE TABLE GOVERNANCE.EXPORT_HISTORY (
    HISTORY_ID VARCHAR(36) PRIMARY KEY,
    EXPORT_ID VARCHAR(36) NOT NULL,

    -- Snapshot of what was sent
    PROVIDER_ID VARCHAR(50) NOT NULL,
    DESTINATION VARCHAR(30) NOT NULL,
    PAYLOAD_HASH VARCHAR(64),                     -- SHA256 of payload for dedup
    PAYLOAD VARIANT NOT NULL,

    -- When/how
    SENT_AT TIMESTAMP_NTZ NOT NULL,
    EXTERNAL_ID VARCHAR(100),

    -- Source tracking
    DATA_LOAD_ID VARCHAR(36),

    -- Outcome
    DELIVERY_STATUS VARCHAR(20),                  -- delivered, bounced, complained, unsubscribed
    DELIVERY_STATUS_AT TIMESTAMP_NTZ,

    -- Costs
    ESTIMATED_COST_USD FLOAT,
    ACTUAL_COST_USD FLOAT,

    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- 5. DESTINATION CONFIG
-- ============================================================================
-- Configuration for each export destination

CREATE OR REPLACE TABLE GOVERNANCE.DESTINATION_CONFIG (
    DESTINATION VARCHAR(30) PRIMARY KEY,
    DISPLAY_NAME VARCHAR(100),

    -- Cost
    COST_PER_RECORD_USD FLOAT DEFAULT 0,

    -- Capabilities
    IS_REVERSIBLE BOOLEAN DEFAULT FALSE,          -- Can we delete/unsend?
    SUPPORTS_BATCH BOOLEAN DEFAULT TRUE,
    MAX_BATCH_SIZE INT DEFAULT 1000,

    -- Approval settings
    AUTO_APPROVE BOOLEAN DEFAULT FALSE,
    MIN_CONFIDENCE_FOR_AUTO INT DEFAULT 85,       -- Minimum match confidence for auto-approve
    DELAY_HOURS INT DEFAULT 0,                    -- Hold period before sending

    -- Rate limits
    RATE_LIMIT_PER_HOUR INT,
    RATE_LIMIT_PER_DAY INT,

    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert destination configurations
INSERT INTO GOVERNANCE.DESTINATION_CONFIG
(DESTINATION, DISPLAY_NAME, COST_PER_RECORD_USD, IS_REVERSIBLE, AUTO_APPROVE, MIN_CONFIDENCE_FOR_AUTO, DELAY_HOURS) VALUES
('ghl', 'GoHighLevel CRM', 0, TRUE, TRUE, 70, 0),
('instantly', 'Instantly (Cold Email)', 0.01, FALSE, TRUE, 85, 0),
('lob_postcard', 'Lob Postcard', 0.50, FALSE, FALSE, 95, 24),
('lob_letter', 'Lob Letter', 1.50, FALSE, FALSE, 95, 48),
('webhook', 'Custom Webhook', 0, FALSE, TRUE, 70, 0);

-- ============================================================================
-- 6. SUPPRESSION LIST
-- ============================================================================
-- Global suppression - never export these

CREATE OR REPLACE TABLE GOVERNANCE.SUPPRESSION_LIST (
    SUPPRESSION_ID VARCHAR(36) PRIMARY KEY,

    -- What to suppress (at least one required)
    EMAIL VARCHAR(255),
    PHONE VARCHAR(20),
    LICENSE_NUMBER VARCHAR(50),
    NPI VARCHAR(10),

    -- Scope
    DESTINATION VARCHAR(30),                      -- NULL = all destinations

    -- Reason
    SUPPRESSION_TYPE VARCHAR(30) NOT NULL,        -- 'bounce', 'complaint', 'unsubscribe', 'manual', 'legal'
    REASON TEXT,

    -- Source
    SOURCE VARCHAR(50),                           -- Where suppression came from
    SOURCE_EXPORT_ID VARCHAR(36),                 -- If from a bounce/complaint

    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EXPIRES_AT TIMESTAMP_NTZ                      -- NULL = permanent
);

CREATE OR REPLACE INDEX IDX_SUPP_EMAIL ON GOVERNANCE.SUPPRESSION_LIST (EMAIL) WHERE EMAIL IS NOT NULL;
CREATE OR REPLACE INDEX IDX_SUPP_LICENSE ON GOVERNANCE.SUPPRESSION_LIST (LICENSE_NUMBER) WHERE LICENSE_NUMBER IS NOT NULL;

-- ============================================================================
-- 7. HELPFUL VIEWS
-- ============================================================================

-- Loads pending promotion (past hold period)
CREATE OR REPLACE VIEW GOVERNANCE.V_LOADS_READY_TO_PROMOTE AS
SELECT *
FROM GOVERNANCE.DATA_LOADS
WHERE STATUS = 'validated'
  AND VALIDATION_PASSED = TRUE
  AND TIMESTAMPADD(HOUR, PROMOTION_DELAY_HOURS, VALIDATED_AT) <= CURRENT_TIMESTAMP();

-- Exports ready to send
CREATE OR REPLACE VIEW GOVERNANCE.V_EXPORTS_READY_TO_SEND AS
SELECT eq.*
FROM GOVERNANCE.EXPORT_QUEUE eq
JOIN GOVERNANCE.DATA_LOADS dl ON eq.DATA_LOAD_ID = dl.LOAD_ID
JOIN GOVERNANCE.DESTINATION_CONFIG dc ON eq.DESTINATION = dc.DESTINATION
WHERE eq.STATUS = 'approved'
  AND dl.STATUS = 'promoted'
  AND dc.IS_ACTIVE = TRUE
  AND (eq.SCHEDULED_SEND_AT IS NULL OR eq.SCHEDULED_SEND_AT <= CURRENT_TIMESTAMP())
  AND NOT EXISTS (
      SELECT 1 FROM GOVERNANCE.SUPPRESSION_LIST sl
      WHERE sl.IS_ACTIVE = TRUE
        AND (sl.DESTINATION IS NULL OR sl.DESTINATION = eq.DESTINATION)
        AND (sl.EXPIRES_AT IS NULL OR sl.EXPIRES_AT > CURRENT_TIMESTAMP())
        AND (
            sl.LICENSE_NUMBER = eq.PAYLOAD:license_number::VARCHAR
            OR sl.EMAIL = eq.PAYLOAD:email::VARCHAR
            OR sl.NPI = eq.PAYLOAD:npi::VARCHAR
        )
  );

-- Daily export summary
CREATE OR REPLACE VIEW GOVERNANCE.V_EXPORT_DAILY_SUMMARY AS
SELECT
    DATE(SENT_AT) AS send_date,
    DESTINATION,
    COUNT(*) AS total_sent,
    SUM(ESTIMATED_COST_USD) AS estimated_cost,
    SUM(CASE WHEN DELIVERY_STATUS = 'bounced' THEN 1 ELSE 0 END) AS bounces,
    SUM(CASE WHEN DELIVERY_STATUS = 'complained' THEN 1 ELSE 0 END) AS complaints
FROM GOVERNANCE.EXPORT_HISTORY
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- Load health dashboard
CREATE OR REPLACE VIEW GOVERNANCE.V_LOAD_HEALTH AS
SELECT
    SOURCE_TYPE,
    SOURCE_STATE,
    MAX(SOURCE_DATE) AS latest_source_date,
    MAX(LOADED_AT) AS latest_load,
    COUNT(CASE WHEN STATUS = 'promoted' THEN 1 END) AS promoted_loads,
    COUNT(CASE WHEN STATUS = 'quarantined' THEN 1 END) AS quarantined_loads,
    AVG(ROW_COUNT) AS avg_row_count,
    AVG(ROW_COUNT_DELTA_PCT) AS avg_delta_pct
FROM GOVERNANCE.DATA_LOADS
WHERE LOADED_AT >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY 1, 2;

-- ============================================================================
-- 8. STORED PROCEDURES
-- ============================================================================

-- Quarantine a load and cancel pending exports
CREATE OR REPLACE PROCEDURE GOVERNANCE.QUARANTINE_LOAD(
    P_LOAD_ID VARCHAR,
    P_REASON VARCHAR,
    P_USER VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    v_cancelled INT;
    v_result VARIANT;
BEGIN
    -- Update load status
    UPDATE GOVERNANCE.DATA_LOADS
    SET STATUS = 'quarantined',
        QUARANTINED_AT = CURRENT_TIMESTAMP(),
        QUARANTINED_BY = P_USER,
        QUARANTINE_REASON = P_REASON,
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE LOAD_ID = P_LOAD_ID;

    -- Cancel pending exports from this load
    UPDATE GOVERNANCE.EXPORT_QUEUE
    SET STATUS = 'cancelled',
        ERROR_MESSAGE = 'Source load quarantined: ' || P_REASON,
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE DATA_LOAD_ID = P_LOAD_ID
      AND STATUS IN ('queued', 'approved', 'scheduled');

    v_cancelled := SQLROWCOUNT;

    -- Update load with cancellation count
    UPDATE GOVERNANCE.DATA_LOADS
    SET EXPORTS_CANCELLED = v_cancelled
    WHERE LOAD_ID = P_LOAD_ID;

    v_result := OBJECT_CONSTRUCT(
        'load_id', P_LOAD_ID,
        'status', 'quarantined',
        'exports_cancelled', v_cancelled
    );

    RETURN v_result;
END;
$$;

-- Promote a validated load
CREATE OR REPLACE PROCEDURE GOVERNANCE.PROMOTE_LOAD(
    P_LOAD_ID VARCHAR,
    P_USER VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    v_status VARCHAR;
    v_validated BOOLEAN;
BEGIN
    -- Check current status
    SELECT STATUS, VALIDATION_PASSED INTO v_status, v_validated
    FROM GOVERNANCE.DATA_LOADS
    WHERE LOAD_ID = P_LOAD_ID;

    IF (v_status != 'validated') THEN
        RETURN OBJECT_CONSTRUCT('error', 'Load must be in validated status to promote');
    END IF;

    IF (v_validated != TRUE) THEN
        RETURN OBJECT_CONSTRUCT('error', 'Load did not pass validation');
    END IF;

    -- Promote
    UPDATE GOVERNANCE.DATA_LOADS
    SET STATUS = 'promoted',
        PROMOTED_AT = CURRENT_TIMESTAMP(),
        PROMOTED_BY = P_USER,
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE LOAD_ID = P_LOAD_ID;

    RETURN OBJECT_CONSTRUCT(
        'load_id', P_LOAD_ID,
        'status', 'promoted',
        'promoted_by', P_USER
    );
END;
$$;

-- ============================================================================
-- GRANTS (adjust role names as needed)
-- ============================================================================
/*
GRANT USAGE ON SCHEMA GOVERNANCE TO ROLE DATA_ENGINEER;
GRANT SELECT ON ALL TABLES IN SCHEMA GOVERNANCE TO ROLE DATA_ENGINEER;
GRANT INSERT, UPDATE ON GOVERNANCE.DATA_LOADS TO ROLE DATA_ENGINEER;
GRANT INSERT, UPDATE ON GOVERNANCE.EXPORT_QUEUE TO ROLE DATA_ENGINEER;
GRANT INSERT ON GOVERNANCE.EXPORT_HISTORY TO ROLE DATA_ENGINEER;
GRANT INSERT ON GOVERNANCE.SUPPRESSION_LIST TO ROLE DATA_ENGINEER;

GRANT USAGE ON SCHEMA GOVERNANCE TO ROLE DATA_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA GOVERNANCE TO ROLE DATA_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA GOVERNANCE TO ROLE DATA_ANALYST;
*/
