-- BigQuery version of NPI load script
LOAD DATA INTO `dental_leads.raw_npi_provider`
FROM FILES (
  format = 'CSV',
  uris = ['gs://dl-ingestion-lake/npi/*.csv.gz']
);
