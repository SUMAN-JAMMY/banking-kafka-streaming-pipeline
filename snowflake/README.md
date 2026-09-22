# 🚀 Real-Time Banking Data Streaming Pipeline

A real-time data engineering pipeline that simulates banking transactions, streams them through **Apache Kafka**, processes them using **Apache Spark Structured Streaming**, stores the processed data in **Amazon S3**, and loads it into **Snowflake** using a Medallion Architecture.

The project demonstrates an end-to-end modern data engineering workflow from **event generation → streaming → processing → cloud storage → data warehouse → analytics**.

---

## 🏗️ Architecture

```text
                    BANKING APPLICATION
                           │
                           │ Transaction Events
                           ▼
                  ┌───────────────────┐
                  │  Python Producer  │
                  │   Fake Generator  │
                  └─────────┬─────────┘
                            │
                            ▼
                  ┌───────────────────┐
                  │   Apache Kafka    │
                  │                   │
                  │   transactions    │
                  └─────────┬─────────┘
                            │
                            ▼
               ┌─────────────────────────┐
               │ Apache Spark Structured │
               │       Streaming         │
               │                         │
               │ • JSON Parsing          │
               │ • Validation             │
               │ • Filtering              │
               └────────────┬────────────┘
                            │
                            ▼
                  ┌───────────────────┐
                  │    Amazon S3      │
                  │                   │
                  │ Parquet Files     │
                  └─────────┬─────────┘
                            │
                            ▼
                  ┌───────────────────┐
                  │     Snowflake     │
                  │                   │
                  │      RAW          │
                  │       ↓           │
                  │    STAGING        │
                  │       ↓           │
                  │      MART         │
                  └─────────┬─────────┘
                            │
                            ▼
                  ┌───────────────────┐
                  │     Metabase      │
                  │    Analytics      │
                  └───────────────────┘
🎯 Project Objective
The goal of this project is to build a realistic real-time banking data pipeline capable of continuously processing transaction events.
The project demonstrates how modern data engineering systems can handle streaming data using:

Python
Apache Kafka
Apache Spark
Amazon S3
Snowflake
SQL
Docker
Git/GitHub
🔄 Data Flow
The pipeline follows this flow:
Python Transaction Generator
            ↓
       Kafka Producer
            ↓
      Kafka Topic
     "transactions"
            ↓
  Spark Structured Streaming
            ↓
     Data Validation
            ↓
        Amazon S3
        Parquet
            ↓
      Snowflake RAW
            ↓
    Snowflake STAGING
            ↓
      Snowflake MART
            ↓
       Analytics
🧰 Technology Stack
Technology	Purpose
Python	Transaction generation
kafka-python	Kafka producer/consumer
Apache Kafka	Real-time event streaming
Apache Spark	Stream processing
PySpark	Python interface for Spark
Amazon S3	Cloud data lake/storage
Snowflake	Cloud data warehouse
SQL	Data transformation
Docker	Containerization
Metabase	Data visualization
Git/GitHub	Version control
📂 Project Structure
banking-kafka-project/
│
├── producers/
│   └── producer.py
│
├── consumers/
│   └── consumer.py
│
├── spark/
│   └── streaming.py
│
├── snowflake/
│   ├── 01_setup.sql
│   ├── 02_raw.sql
│   ├── 03_staging.sql
│   └── 04_mart.sql
│
├── requirements.txt
├── .gitignore
└── README.md
1️⃣ Python Transaction Producer
The Python producer simulates a banking application generating transactions continuously.
Each transaction contains:

{
  "transaction_id": 123456,
  "account_id": 1234,
  "type": "TRANSFER",
  "amount": 4500.50,
  "timestamp": "2026-09-22T10:30:00"
}
The producer sends a new transaction to Kafka every few seconds.
Transaction Types
The generator currently supports:
TRANSFER
PAYMENT
CASH_OUT
DEBIT
2️⃣ Apache Kafka
Kafka acts as the real-time event streaming platform.
The project uses the following Kafka topic:

transactions
Kafka receives transaction events from the Python producer and makes them available to downstream consumers such as Spark.
Kafka Architecture
Producer
   │
   ▼
Kafka Broker
   │
   ▼
transactions topic
   │
   ├── Python Consumer
   │
   └── Spark Streaming
Kafka is responsible for transporting and temporarily storing the streaming events.
3️⃣ Spark Structured Streaming
Apache Spark processes the Kafka stream in real time.
Spark performs:

Kafka data ingestion
JSON parsing
Schema enforcement
Data validation
Filtering
Writing processed data to S3
Validation Rules
Transactions are accepted only when:
amount > 0
and
account_id IS NOT NULL
This ensures that invalid transactions are filtered before entering the data lake.
4️⃣ Amazon S3
Processed streaming data is stored in Amazon S3.
The data is written in Parquet format.

Amazon S3
│
└── banking-kafka-data-jammy/
    │
    ├── transactions/
    │   ├── part-xxxxx.snappy.parquet
    │   ├── part-xxxxx.snappy.parquet
    │   └── ...
    │
    └── checkpoint/
Spark checkpointing is used to maintain streaming state and support reliable processing.
5️⃣ Snowflake
Snowflake is used as the cloud data warehouse.
The project follows a simple Medallion Architecture:

RAW
 ↓
STAGING
 ↓
MART
Database Structure
BANKING
│
├── RAW
│   └── TRANSACTIONS_RAW
│
├── STAGING
│   └── TRANSACTIONS_STAGING
│
└── MART
    └── TRANSACTIONS_MART
🥉 RAW Layer
The RAW layer stores data loaded from Amazon S3 with minimal transformation.
S3 → TRANSACTIONS_RAW
Purpose:
Preserve source data
Maintain historical records
Provide a reliable ingestion layer
🥈 STAGING Layer
The STAGING layer performs basic cleaning and validation.
Example:

SELECT
    transaction_id,
    account_id,
    type,
    amount,
    timestamp
FROM transactions_raw
WHERE amount > 0
  AND account_id IS NOT NULL;
🥇 MART Layer
The MART layer contains analytics-ready data.
Example aggregation:

SELECT
    type,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS average_amount
FROM transactions_staging
GROUP BY type;
This allows analysts to easily answer questions such as:
How many transactions occurred?
What is the total transaction amount?
What is the average transaction amount?
Which transaction types are most common?
🔐 Security
Sensitive credentials are not committed to GitHub.
The .gitignore file excludes:

.venv/
.env
*.pem
*.p8
*.pyc
__pycache__/
.DS_Store
data/*.csv
AWS credentials, Snowflake credentials, private keys, and environment files should never be pushed to GitHub.
⚙️ Local Setup
1. Clone the repository
git clone https://github.com/SUMAN-JAMMY/banking-kafka-streaming-pipeline.git
cd banking-kafka-streaming-pipeline
2. Create virtual environment
Using uv:
uv venv
Activate it:
source .venv/bin/activate
3. Install dependencies
uv pip install -r requirements.txt
PySpark can be installed using:
uv pip install pyspark
🚀 Running the Pipeline
The pipeline requires separate terminals for the different services.
Terminal 1 — Start Kafka
Navigate to the Kafka installation:
cd ~/Desktop/kafka-project/kafka_2.13-4.3.1
Start Kafka:
bin/kafka-server-start.sh config/server.properties
Terminal 2 — Start Producer
From the project directory:
cd ~/Desktop/banking-kafka-project
Activate the environment:
source .venv/bin/activate
Run:
python producers/producer.py
You should see:
Sent: {'transaction_id': ..., 'account_id': ..., ...}
Terminal 3 — Run Spark Streaming
From the project directory:
cd ~/Desktop/banking-kafka-project
Run:
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.hadoop:hadoop-aws:3.4.1 \
spark/streaming.py
Spark will:
Kafka
  ↓
Parse JSON
  ↓
Validate transactions
  ↓
Write Parquet
  ↓
Amazon S3
🧪 Testing Kafka
The project also contains a simple Kafka consumer.
Run:

python consumers/consumer.py
Expected output:
Received: {
    'transaction_id': ...,
    'account_id': ...,
    'type': 'TRANSFER',
    'amount': ...,
    'timestamp': ...
}
☁️ AWS Configuration
The project uses Amazon S3 as the cloud storage layer.
AWS CLI can be configured using:

aws configure
Verify the AWS identity:
aws sts get-caller-identity
The S3 bucket used by the project is:
banking-kafka-data-jammy
❄️ Snowflake Configuration
Snowflake is used as the analytical warehouse.
The project creates:

BANKING
│
├── RAW
├── STAGING
└── MART
An external Snowflake stage connects Snowflake to the S3 bucket.
The general flow is:

Amazon S3
    ↓
Snowflake External Stage
    ↓
TRANSACTIONS_RAW
    ↓
TRANSACTIONS_STAGING
    ↓
TRANSACTIONS_MART
The Snowflake SQL scripts are available under:
snowflake/
📊 Analytics
The MART layer can be connected to a BI/analytics tool such as Metabase.
Potential dashboards include:

Transaction Overview
Total transactions
Total transaction value
Average transaction amount
Transactions by type
Transaction Analysis
Transfer volume
Payment volume
Debit volume
Cash-out volume
Time Analysis
Transactions over time
Transaction value over time
Average transaction value
🧠 Key Data Engineering Concepts Demonstrated
This project demonstrates practical understanding of:
Streaming
Event-driven architecture
Kafka producers
Kafka consumers
Kafka topics
Real-time streaming
Processing
Spark Structured Streaming
JSON parsing
Schema enforcement
Data validation
Micro-batch processing
Cloud
AWS S3
IAM
S3 data lake
Snowflake external stages
Cloud data warehouse
Data Architecture
Medallion Architecture
RAW layer
STAGING layer
MART layer
Data Engineering
ETL/ELT concepts
Streaming pipelines
Data quality
Batch/micro-batch processing
Cloud storage
Analytical data modeling
🔮 Future Improvements
Possible future enhancements:
 Add multiple Kafka topics
 Add payment and login events
 Add Kafka partitions
 Add multiple Kafka brokers
 Implement real-time fraud detection
 Add Apache Airflow orchestration
 Add automated Snowflake ingestion
 Add Snowflake Streams & Tasks
 Add CI/CD pipeline
 Add Metabase dashboards
 Add monitoring and logging
 Deploy Kafka/Spark infrastructure on AWS
 Add data quality checks
 Implement schema evolution
📌 Project Status
Current Pipeline
✅ Python Transaction Generator
        ↓
✅ Apache Kafka
        ↓
✅ Spark Structured Streaming
        ↓
✅ Amazon S3
        ↓
✅ Snowflake RAW
        ↓
✅ Snowflake STAGING
        ↓
✅ Snowflake MART
        ↓
🔄 Metabase Analytics
👨‍💻 Author
Suman Jena
B.Tech — Computer Science & Engineering (Data Science)

Interested in:

Data Engineering
Data Science
Cloud Data Platforms
Real-Time Data Processing
Big Data
AI/ML
⭐ If you find this project useful
Feel free to explore the repository, raise issues, or suggest improvements.

### One small thing I'd change before you push it

Since your project is actually **working end-to-end up to Snowflake**, this README presents it much better than calling it just a "Kafka project." It's really a **real-time data engineering pipeline**.

For GitHub, I'd also add these badges at the top later:

```markdown
![Python](https://img.shields.io/badge/Python-3.x-blue)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-4.x-black)
![Spark](https://img.shields.io/badge/Apache%20Spark-4.2-orange)
![AWS S3](https://img.shields.io/badge/AWS-S3-yellow)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-blue)
