import os
import io
import uuid
from datetime import datetime, timedelta, timezone, date
from typing import List

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.cloud import bigquery


BINANCE_BASE_URL = "https://api.binance.com"


S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-west-2")
BRONZE_PREFIX = "bronze_trade_event"


BQ_PROJECT_ID = os.getenv("BQ_PROJECT")
BQ_DATASET_ID = "binance_data"
BQ_TABLE_ID = "bronze_trades_raw"


def _get_s3_client():
   
    return boto3.client("s3", region_name=AWS_REGION)

def _get_bq_client():
    
    return bigquery.Client(project=BQ_PROJECT_ID)


def _fetch_trades(symbol: str, start_ts: int, end_ts: int, limit: int = 1000) -> List[dict]:
    url = f"{BINANCE_BASE_URL}/api/v3/aggTrades"
    params = {"symbol": symbol, "startTime": start_ts, "endTime": end_ts, "limit": limit}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"API Error for {symbol}: {e}")
        return []

def _process_dataframe(df: pd.DataFrame, event_date: date) -> pd.DataFrame:
  
    if df.empty:
        return df
    
    
    df["event_date"] = pd.to_datetime(event_date)
    df["event_time"] = pd.to_datetime(df["T"], unit="ms", utc=True)
    df["price"] = pd.to_numeric(df["p"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["q"], errors="coerce")
    df["trade_id"] = df["a"].astype(str)
    df["ingest_time"] = datetime.now(timezone.utc)

   
    cols = ["trade_id", "event_date", "event_time", "symbol", "price", "quantity", "ingest_time"]
    return df[[c for c in cols if c in df.columns]]

def _write_to_s3(df: pd.DataFrame, event_date: date, hour: int) -> None:
   
    if df.empty: return

    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    date_str = event_date.isoformat()

    key = f"{BRONZE_PREFIX}/date={date_str}/hour={hour:02d}/part-{uuid.uuid4().hex}.parquet"
    
    try:
        s3 = _get_s3_client()
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
        print(f"[AWS] Uploaded to s3://{S3_BUCKET}/{key}")
    except Exception as e:
        print(f"[AWS] S3 Upload Error: {e}")

def _write_to_bigquery(df: pd.DataFrame) -> None:
    """写入 Google BigQuery (Data Warehouse)"""
    if df.empty: return

    client = _get_bq_client()
    table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        autodetect=True,
    )

    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"[GCP] Loaded {len(df)} rows to BigQuery table {table_ref}")
    except Exception as e:
        print(f"[GCP] BigQuery Load Error: {e}")

def ingest_last_3_days(end_date_str: str, days: int = 3) -> None:
    if not BQ_PROJECT_ID or not S3_BUCKET:
        raise ValueError("Missing BQ_PROJECT or S3_BUCKET env vars")

    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    start_date = end_date - timedelta(days=days - 1)
    symbols = ["BTCUSDT", "ETHUSDT"] 

    print(f"Pipeline: API -> Dual Write (AWS S3 + Google BigQuery)")

    for offset in range(days):
        current_date = start_date + timedelta(days=offset)

        for hour in range(12, 13): 
            start_dt = datetime(
                current_date.year, current_date.month, current_date.day,
                hour, 0, 0, tzinfo=timezone.utc
            )
            end_dt = start_dt + timedelta(hours=1)
            start_ts = int(start_dt.timestamp() * 1000)
            end_ts = int(end_dt.timestamp() * 1000)

            frames = []
            for symbol in symbols:
                trades = _fetch_trades(symbol, start_ts, end_ts)
                if trades:
                    raw_df = pd.DataFrame(trades)
                    raw_df["symbol"] = symbol
                    frames.append(raw_df)
            
            if frames:
                
                full_df = pd.concat(frames, ignore_index=True)
                clean_df = _process_dataframe(full_df, current_date)
                
              
                _write_to_s3(clean_df, current_date, hour)
                
              
                _write_to_bigquery(clean_df)
            else:
                print(f"No data for {current_date} hour {hour}")

if __name__ == "__main__":
  
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    

    ingest_last_3_days(yesterday, 1)