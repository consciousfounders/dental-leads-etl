-- BigQuery: Load NPI data from GCS
-- Run with: bq query --use_legacy_sql=false < sql/raw/load_npi.bigquery.sql

-- Create RAW schema (dataset already created: dental_leads)
-- External table pointing directly to GCS (no data copy, always fresh)

CREATE OR REPLACE EXTERNAL TABLE `silicon-will-480022-f8.dental_leads.npi_raw`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dl-ingestion-lake/npi/npidata_pfile_20050523-20251109.csv.gz'],
  skip_leading_rows = 1,
  allow_quoted_newlines = true,
  allow_jagged_rows = true
);

-- Or load into native table for better performance:
/*
CREATE OR REPLACE TABLE `silicon-will-480022-f8.dental_leads.npi_raw` AS
SELECT * FROM `silicon-will-480022-f8.dental_leads.npi_raw_external`;
*/
