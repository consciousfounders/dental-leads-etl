-- Snowflake version of NPI load script
COPY INTO RAW.NPI_PROVIDER
FROM @npi_stage
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"')
PATTERN='.*\.csv\.gz';
