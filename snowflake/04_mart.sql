CREATE OR REPLACE TABLE transactions_mart AS
SELECT
    "type",
    COUNT(*) AS transaction_count,
    SUM("amount") AS total_amount,
    AVG("amount") AS average_amount
FROM transactions_staging
GROUP BY "type";
SELECT * FROM transactions_mart;