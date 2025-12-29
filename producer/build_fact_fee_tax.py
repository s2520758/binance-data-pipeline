import argparse
import os

import pandas as pd
from google.cloud import bigquery


def load_silver_for_date(bucket: str, prefix: str, process_date: str) -> pd.DataFrame:
    base = f"s3://{bucket}/{prefix}/date={process_date}/"
    return pd.read_parquet(base)


def load_rules(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def build_fact(df_silver: pd.DataFrame, df_rules: pd.DataFrame, default_region: str) -> pd.DataFrame:
    df = df_silver.copy()
    df["region"] = default_region

    df_rules_keyed = df_rules[["symbol", "region", "fee_rate_bps", "tax_rate_bps"]]
    df = df.merge(
        df_rules_keyed,
        on=["symbol", "region"],
        how="left",
        suffixes=("", "_rule"),
    )

    df["fee_rate_bps"] = df["fee_rate_bps"].fillna(0.0)
    df["tax_rate_bps"] = df["tax_rate_bps"].fillna(0.0)

    df["fee_amount"] = df["notional"] * df["fee_rate_bps"] / 10000.0
    df["tax_amount"] = df["notional"] * df["tax_rate_bps"] / 10000.0

    cols = [
        "symbol",
        "region",
        "event_ts",
        "trade_ts",
        "price",
        "quantity",
        "notional",
        "fee_rate_bps",
        "fee_amount",
        "tax_rate_bps",
        "tax_amount",
        "is_buyer_maker",
        "trade_id",
    ]
    existing_cols = [c for c in cols if c in df.columns]
    df = df[existing_cols]

    return df


def write_fact_to_s3(df_fact: pd.DataFrame, bucket: str, prefix: str, process_date: str) -> None:
    base = f"s3://{bucket}/{prefix}/date={process_date}/"
    df_fact.to_parquet(base, index=False)


def write_fact_to_bigquery(df_fact: pd.DataFrame) -> None:
    project = os.getenv("BQ_PROJECT")
    dataset = os.getenv("BQ_DATASET", "binance_revenue")
    table = os.getenv("BQ_FACT_TABLE", "fact_trade_fee_tax")

    if not project:
        return

    client = bigquery.Client(project=project)

    df_bq = df_fact.copy()
    df_bq["event_date"] = df_bq["event_ts"].dt.date

    cols = [
        "event_date",
        "symbol",
        "region",
        "event_ts",
        "trade_ts",
        "price",
        "quantity",
        "notional",
        "fee_rate_bps",
        "fee_amount",
        "tax_rate_bps",
        "tax_amount",
        "is_buyer_maker",
        "trade_id",
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
    silver_prefix = os.getenv("SILVER_PREFIX", "silver_trade_event")
    fact_prefix = os.getenv("FACT_PREFIX", "fact_trade_fee_tax")
    rules_path = os.getenv("FEE_TAX_RULES_PATH", "config/fee_tax_rules.csv")
    default_region = os.getenv("DEFAULT_REGION", "GLOBAL")

    print(f"reading silver for date={process_date} from S3...")
    df_silver = load_silver_for_date(bucket, silver_prefix, process_date)
    print(f"silver rows: {len(df_silver)}")

    df_rules = load_rules(rules_path)
    df_fact = build_fact(df_silver, df_rules, default_region)
    print(f"fact rows: {len(df_fact)}")

    print("writing fact to S3...")
    write_fact_to_s3(df_fact, bucket, fact_prefix, process_date)

    print("writing fact to BigQuery...")
    write_fact_to_bigquery(df_fact)
    print("done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    main(args.date)

