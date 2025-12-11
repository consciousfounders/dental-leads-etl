-- ============================================================
-- Mental Health Provider Database Schema
-- ============================================================
-- Target Market: Therapists, Psychologists, Psychiatrists,
-- Mental Health Facilities, Behavioral Health Clinics,
-- Rehabilitation Centers, Substance Abuse Treatment
-- ============================================================

-- Create dedicated schema
CREATE SCHEMA IF NOT EXISTS MENTAL_HEALTH;

-- Taxonomy code reference table
CREATE OR REPLACE TABLE MENTAL_HEALTH.TAXONOMY_CODES (
    TAXONOMY_CODE VARCHAR(20) PRIMARY KEY,
    CATEGORY VARCHAR(50),
    SPECIALTY_NAME VARCHAR(100),
    DESCRIPTION VARCHAR(500),
    IS_FACILITY BOOLEAN DEFAULT FALSE
);

-- Insert mental health taxonomy codes
INSERT INTO MENTAL_HEALTH.TAXONOMY_CODES VALUES
-- Counselors (101Y)
('101Y00000X', 'Counselor', 'Counselor (General)', 'Licensed counselor providing mental health services', FALSE),
('101YM0800X', 'Counselor', 'Mental Health Counselor', 'Licensed mental health counselor', FALSE),
('101YA0400X', 'Counselor', 'Addiction Counselor', 'Substance abuse and addiction counselor', FALSE),
('101YP2500X', 'Counselor', 'Professional Counselor', 'Licensed professional counselor', FALSE),
('101YP1600X', 'Counselor', 'Pastoral Counselor', 'Pastoral/spiritual counselor', FALSE),
('101YS0200X', 'Counselor', 'School Counselor', 'School-based counselor', FALSE),

-- Clinical Social Workers (1041C)
('1041C0700X', 'Social Worker', 'Clinical Social Worker', 'Licensed clinical social worker (LCSW)', FALSE),

-- Marriage & Family Therapists (106H)
('106H00000X', 'Therapist', 'Marriage & Family Therapist', 'Licensed marriage and family therapist (LMFT)', FALSE),

-- Psychologists (103T)
('103T00000X', 'Psychologist', 'Psychologist (General)', 'Licensed psychologist', FALSE),
('103TC0700X', 'Psychologist', 'Clinical Psychologist', 'Clinical psychologist', FALSE),
('103TC1900X', 'Psychologist', 'Counseling Psychologist', 'Counseling psychologist', FALSE),
('103TC2200X', 'Psychologist', 'Child Psychologist', 'Child & adolescent psychologist', FALSE),
('103TS0200X', 'Psychologist', 'School Psychologist', 'School psychologist', FALSE),
('103TB0200X', 'Psychologist', 'Behavioral Psychologist', 'Behavioral psychologist', FALSE),
('103TA0400X', 'Psychologist', 'Addiction Psychologist', 'Addiction psychologist', FALSE),

-- Psychiatrists (2084P)
('2084P0800X', 'Psychiatrist', 'Psychiatrist', 'Medical doctor specializing in psychiatry', FALSE),
('2084P0804X', 'Psychiatrist', 'Addiction Psychiatrist', 'Addiction psychiatry specialist', FALSE),

-- Mental Health Facilities (261QM)
('261QM0801X', 'Facility', 'Mental Health Clinic', 'Community mental health center', TRUE),
('261QM0850X', 'Facility', 'Adult Mental Health Clinic', 'Adult mental health program', TRUE),
('261QM0855X', 'Facility', 'Child Mental Health Clinic', 'Child/adolescent mental health program', TRUE),
('261QM1300X', 'Facility', 'Multi-Specialty Mental Health', 'Multi-specialty mental health clinic', TRUE),
('261QM1200X', 'Facility', 'MHMR Center', 'Mental health/mental retardation center', TRUE),
('261QM2500X', 'Facility', 'Medical Specialty Clinic', 'Medical specialty mental health clinic', TRUE),
('261QM2800X', 'Facility', 'Methadone Clinic', 'Methadone treatment clinic', TRUE),

-- Rehabilitation & Substance Abuse (261QR, 324500)
('261QR0400X', 'Facility', 'Rehabilitation Clinic', 'Rehabilitation clinic/center', TRUE),
('261QR0405X', 'Facility', 'Substance Use Rehab', 'Substance use disorder rehabilitation', TRUE),
('261QR1300X', 'Facility', 'Rural Health Clinic', 'Rural health clinic with mental health', TRUE),
('261QR0200X', 'Facility', 'Physical Rehab Clinic', 'Physical therapy rehabilitation', TRUE),
('324500000X', 'Facility', 'Substance Abuse Facility', 'Substance abuse treatment facility', TRUE),

-- Psychiatric Hospitals (283Q)
('283Q00000X', 'Facility', 'Psychiatric Hospital', 'Psychiatric hospital', TRUE);

