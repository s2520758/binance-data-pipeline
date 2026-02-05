import os

AWS_REGION = os.getenv("AWS_REGION", "eu-west-2")
S3_BUCKET = os.getenv("S3_BUCKET", "ealtime-revenue-binance")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "bronze_trade_event")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "silver_trade_event")
FACT_PREFIX = os.getenv("FACT_PREFIX", "fact_trade_fee_tax")


S3_BUCKET = os.getenv("S3_BUCKET", "your-s3-bucket-name") 
BQ_PROJECT = os.getenv("BQ_PROJECT", "your-gcp-project-id")