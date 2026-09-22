CREATE OR REPLACE TABLE transactions_staging AS
SELECT
    "transaction_id",
    "account_id",
    "type",
    "amount",
    "timestamp"
FROM transactions_raw
WHERE "amount" > 0
  AND "account_id" IS NOT NULL;
SELECT * FROM transactions_staging LIMIT 10;