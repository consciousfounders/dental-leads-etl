-- BigQuery: Create datasets (schemas) for medallion architecture
-- Note: RAW dataset (dental_leads) already exists with npi_raw table

-- Create CLEAN dataset
CREATE SCHEMA IF NOT EXISTS `silicon-will-480022-f8.dental_leads_clean`
OPTIONS(location="US");

-- Create ENRICHED dataset
CREATE SCHEMA IF NOT EXISTS `silicon-will-480022-f8.dental_leads_enriched`
OPTIONS(location="US");

-- Create SEGMENTED dataset
CREATE SCHEMA IF NOT EXISTS `silicon-will-480022-f8.dental_leads_segmented`
OPTIONS(location="US");

