import io
import uuid
from pathlib import Path
from typing import List

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery

from config import AWS_REGION, S3_BUCKET, FACT_PREFIX, BQ_PROJECT, BQ_DATASET


def _bq_client() -> bigquery.Client:
    return bigquery.Client(project=BQ_PROJECT)


def _s3_client() -> boto3.client:
    return boto3.client("s3", region_name=AWS_REGION)


def _read_silver_from_bigquery(process_date: str) -> pd.DataFrame:
    client = _bq_client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.silver_trade_event"
    query = f"""
    SELECT event_date, symbol, traded_notional
    FROM `{table_id}`
    WHERE event_date = @event_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("event_date", "DATE", process_date)
        ]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    return df


def _load_fee_tax_rules() -> pd.DataFrame:
    here = Path(__file__).resolve().parent
    rules_path = here / "rules" / "fee_tax_rules.csv"
    df_rules = pd.read_csv(rules_path)
    return df_rules


def _build_fact(df_silver: pd.DataFrame, df_rules: pd.DataFrame) -> pd.DataFrame:
    if df_silver.empty:
        return df_silver

    grouped = (
        df_silver.groupby(["event_date", "symbol"], as_index=False)["traded_notional"].sum()
    )

    df_rules = df_rules[["symbol", "region", "fee_rate_bps", "tax_rate_bps"]]
    df = grouped.merge(df_rules, on="symbol", how="left")

    df["region"] = df["region"].fillna("EU")
    df["fee_rate_bps"] = df["fee_rate_bps"].fillna(0.0)
    df["tax_rate_bps"] = df["tax_rate_bps"].fillna(0.0)

    df["fee_revenue"] = df["traded_notional"] * (df["fee_rate_bps"] / 10000.0)
    df["tax_collected"] = df["traded_notional"] * (df["tax_rate_bps"] / 10000.0)

    cols = [
        "event_date",
        "symbol",
        "region",
        "traded_notional",
        "fee_revenue",
        "tax_collected",
    ]
    return df[cols]


def _write_fact_to_bigquery(df_fact: pd.DataFrame) -> None:
    if df_fact.empty:
        return
    client = _bq_client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.fact_trade_fee_tax"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df_fact, table_id, job_config=job_config).result()


def _write_fact_to_s3(df_fact: pd.DataFrame, process_date: str) -> None:
    if df_fact.empty:
        return
    table = pa.Table.from_pandas(df_fact)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    key = f"{FACT_PREFIX}/date={process_date}/fact-{uuid.uuid4().hex}.parquet"
    client = _s3_client()
    client.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())


def main(process_date: str) -> None:
    df_silver = _read_silver_from_bigquery(process_date)
    if df_silver.empty:
        print(f"no silver rows for date={process_date}, skipping fact")
        return
    df_rules = _load_fee_tax_rules()
    df_fact = _build_fact(df_silver, df_rules)
    if df_fact.empty:
        print(f"empty fact for date={process_date}, skipping")
        return
    _write_fact_to_bigquery(df_fact)
    _write_fact_to_s3(df_fact, process_date)
    print(f"fact done for date={process_date}, rows={len(df_fact)}")
