-- ============================================================
-- CLEAN Schema Setup for Dental Leads ETL
-- ============================================================
-- Creates the CLEAN schema and reference tables
-- Run this ONCE before running the transformation scripts
-- ============================================================

-- Create CLEAN schema
CREATE SCHEMA IF NOT EXISTS CLEAN;

-- Create taxonomy reference table for dental specialties
CREATE OR REPLACE TABLE CLEAN.DENTAL_TAXONOMY_CODES (
    taxonomy_code VARCHAR(20) PRIMARY KEY,
    specialty_name VARCHAR(100),
    description VARCHAR(500)
);

-- Insert dental taxonomy codes
INSERT INTO CLEAN.DENTAL_TAXONOMY_CODES (taxonomy_code, specialty_name, description) VALUES
    ('122300000X', 'Dentist', 'General Dentist - A dentist is a person qualified by a doctorate in dental surgery (D.D.S.) or dental medicine (D.M.D.)'),
    ('1223G0001X', 'General Practice', 'Dentist providing general dental services'),
    ('1223X0400X', 'Orthodontics', 'Dentist specializing in the diagnosis, prevention, and correction of malpositioned teeth and jaws'),
    ('1223P0221X', 'Pediatric Dentistry', 'Dentist specializing in the oral health care of infants and children through adolescence'),
    ('1223S0112X', 'Oral & Maxillofacial Surgery', 'Dentist specializing in the diagnosis and surgical treatment of diseases, injuries, and defects of the oral and maxillofacial region'),
    ('1223E0200X', 'Endodontics', 'Dentist specializing in the morphology, physiology, and pathology of the human dental pulp and periradicular tissues'),
    ('1223P0300X', 'Periodontics', 'Dentist specializing in the prevention, diagnosis, and treatment of diseases of the supporting and surrounding tissues of the teeth'),
    ('1223P0700X', 'Prosthodontics', 'Dentist specializing in the restoration and replacement of teeth'),
    ('1223D0001X', 'Dental Public Health', 'Dentist specializing in promoting dental health through organized community efforts'),
    ('1223P0106X', 'Oral & Maxillofacial Pathology', 'Dentist specializing in the nature and identification of diseases affecting the oral and maxillofacial regions'),
    ('1223D0004X', 'Dentist Anesthesiologist', 'Dentist specializing in the management of pain and anxiety during dental procedures'),
    ('1223X0008X', 'Oral & Maxillofacial Radiology', 'Dentist specializing in the production and interpretation of images and data for diagnosis and management of oral and maxillofacial disease'),
    ('1223X2210X', 'Orofacial Pain', 'Dentist specializing in the diagnosis, management, and treatment of pain disorders of the jaw, mouth, face, and associated regions');

-- Verify reference table
SELECT * FROM CLEAN.DENTAL_TAXONOMY_CODES ORDER BY specialty_name;

