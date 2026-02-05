import os

AWS_REGION = os.getenv("AWS_REGION", "eu-west-2")
S3_BUCKET = os.getenv("S3_BUCKET", "ealtime-revenue-binance")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "bronze_trade_event")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "silver_trade_event")
FACT_PREFIX = os.getenv("FACT_PREFIX", "fact_trade_fee_tax")

BQ_PROJECT = os.getenv("BQ_PROJECT", "project-fa672459-e6ce-4619-a6e")
BQ_DATASET = os.getenv("BQ_DATASET", "binance_revenue")
