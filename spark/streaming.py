from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

spark = SparkSession.builder \
    .appName("BankingStreaming") \
    .getOrCreate()

# Transaction structure
schema = StructType([
    StructField("transaction_id", IntegerType()),
    StructField("account_id", IntegerType()),
    StructField("type", StringType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", StringType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value from JSON string to columns
transactions = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Basic cleaning
clean_transactions = transactions.filter(
    (col("amount") > 0) &
    (col("account_id").isNotNull())
)

# Show processed transactions
query = clean_transactions.writeStream \
    .format("parquet") \
    .option("path", "s3a://banking-kafka-data-jammy/transactions/") \
    .option("checkpointLocation", "s3a://banking-kafka-data-jammy/checkpoint/") \
    .outputMode("append") \
    .start()

query.awaitTermination()