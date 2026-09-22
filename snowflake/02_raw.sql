CREATE OR REPLACE FILE FORMAT parquet_ff TYPE = PARQUET;

CREATE OR REPLACE TABLE transactions_raw
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@banking_s3_stage',
            FILE_FORMAT => 'parquet_ff'
        )
    )
);
select *from transactions_raw;
COPY INTO transactions_raw
FROM @banking_s3_stage
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet';