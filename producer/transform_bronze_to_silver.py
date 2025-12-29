import argparse
import os

import pandas as pd
from google.cloud import bigquery


def load_bronze_for_date(bucket: str, prefix: str, process_date: str) -> pd.DataFrame:
    base = f"s3://{bucket}/{prefix}/date={process_date}/"
    return pd.read_parquet(base)


def transform_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

    df["event_ts"] = pd.to_datetime(df["event_time"], unit="ms", utc=True)
    df["trade_ts"] = pd.to_datetime(df["trade_time"], unit="ms", utc=True)

    df["notional"] = df["price"] * df["quantity"]

    df = df.dropna(subset=["symbol", "trade_id"])
    df = df.drop_duplicates(subset=["symbol", "trade_id"])

    cols = [
        "symbol",
        "trade_id",
        "event_ts",
        "trade_ts",
        "price",
        "quantity",
        "notional",
        "is_buyer_maker",
        "stream",
        "event_type",
        "buyer_order_id",
        "seller_order_id",
        "ingest_time",
    ]
    existing_cols = [c for c in cols if c in df.columns]
    df = df[existing_cols]

    return df


def write_silver_to_s3(df: pd.DataFrame, bucket: str, prefix: str, process_date: str) -> None:
    base = f"s3://{bucket}/{prefix}/date={process_date}/"
    df.to_parquet(base, index=False)


def write_silver_to_bigquery(df: pd.DataFrame) -> None:
    project = os.getenv("BQ_PROJECT")
    dataset = os.getenv("BQ_DATASET", "binance_revenue")
    table = os.getenv("BQ_SILVER_TABLE", "silver_trade_event")

    if not project:
        return

    client = bigquery.Client(project=project)

    df_bq = df.copy()
    df_bq["event_date"] = df_bq["event_ts"].dt.date

    cols = [
        "event_date",
        "symbol",
        "trade_id",
        "event_ts",
        "trade_ts",
        "price",
        "quantity",
        "notional",
        "is_buyer_maker",
    ]
    existing_cols = [c for c in cols if c in df_bq.columns]
    df_bq = df_bq[existing_cols]

    table_id = f"{project}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    job = client.load_table_from_dataframe(df_bq, table_id, job_config=job_config)
    job.result()


def main(process_date: str) -> None:
    bucket = os.getenv("S3_BUCKET", "ealtime-revenue-binance")
    bronze_prefix = os.getenv("BRONZE_PREFIX", "bronze_trade_event")
    silver_prefix = os.getenv("SILVER_PREFIX", "silver_trade_event")

    print(f"reading bronze for date={process_date} from S3...")
    df_bronze = load_bronze_for_date(bucket, bronze_prefix, process_date)

    print(f"bronze rows: {len(df_bronze)}")
    df_silver = transform_to_silver(df_bronze)
    print(f"silver rows: {len(df_silver)}")

    print("writing silver to S3...")
    write_silver_to_s3(df_silver, bucket, silver_prefix, process_date)

    print("writing silver to BigQuery...")
    write_silver_to_bigquery(df_silver)
    print("done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="processing date, e.g. 2025-12-28")
    args = parser.parse_args()
    main(args.date)
