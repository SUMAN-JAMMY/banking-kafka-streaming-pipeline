create or replace database banking;
create or replace  schema raw;
use schema raw;
create or replace warehouse compute_wx;
CREATE OR REPLACE STORAGE INTEGRATION banking_s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::619891987400:role/banking-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://banking-kafka-data-jammy/');
USE DATABASE banking;
USE SCHEMA raw;

CREATE OR REPLACE STAGE banking_s3_stage
URL = 's3://banking-kafka-data-jammy/transactions/'
STORAGE_INTEGRATION = banking_s3_int
FILE_FORMAT = (TYPE = PARQUET);
LIST @banking_s3_stage;
DESC INTEGRATION banking_s3_int;
SELECT "property", "property_value"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "property" IN ('STORAGE_AWS_IAM_USER_ARN', 'STORAGE_AWS_EXTERNAL_ID');