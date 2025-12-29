import json
import os
import time
from datetime import datetime
from io import BytesIO
from typing import List

import boto3
import pandas as pd
from confluent_kafka import Consumer, KafkaError, KafkaException

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "binance_trades_raw")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "bronze-s3-writer")

S3_BUCKET = os.getenv("S3_BUCKET", "realtime-revenue-binance")
S3_PREFIX = os.getenv("S3_PREFIX", "bronze_trade_event")


def create_consumer() -> Consumer:
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    return Consumer(config)


def parse_record(msg_value: bytes) -> dict:
    data = json.loads(msg_value.decode("utf-8"))
    return {
        "stream": data.get("stream"),
        "event_type": data.get("event_type"),
        "event_time": data.get("event_time"),
        "symbol": data.get("symbol"),
        "trade_id": data.get("trade_id"),
        "price": data.get("price"),
        "quantity": data.get("quantity"),
        "buyer_order_id": data.get("buyer_order_id"),
        "seller_order_id": data.get("seller_order_id"),
        "trade_time": data.get("trade_time"),
        "is_buyer_maker": data.get("is_buyer_maker"),
        "ignore": data.get("ignore"),
        "ingest_time": data.get("ingest_time"),
    }


def make_s3_key(event_time_ms: int) -> str:
    ts = datetime.utcfromtimestamp(event_time_ms / 1000.0)
    date_part = ts.date().isoformat()
    hour_part = f"{ts.hour:02d}"
    ts_ms = int(time.time() * 1000)
    return f"{S3_PREFIX}/date={date_part}/hour={hour_part}/part-{ts_ms}.parquet"


def flush_batch(s3_client, records: List[dict]) -> None:
    if not records:
        return
    df = pd.DataFrame.from_records(records)
    if df.empty or "event_time" not in df.columns:
        return
    first_ts = int(df["event_time"].dropna().iloc[0])
    key = make_s3_key(first_ts)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=buffer.getvalue())


def main():
    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    s3_client = boto3.client("s3")

    buffer: List[dict] = []
    max_batch_size = 1000
    flush_interval_sec = 5
    last_flush_ts = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                now = time.time()
                if buffer and now - last_flush_ts >= flush_interval_sec:
                    flush_batch(s3_client, buffer)
                    buffer.clear()
                    consumer.commit()
                    last_flush_ts = now
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            record = parse_record(msg.value())
            buffer.append(record)

            now = time.time()
            if len(buffer) >= max_batch_size or now - last_flush_ts >= flush_interval_sec:
                flush_batch(s3_client, buffer)
                buffer.clear()
                consumer.commit()
                last_flush_ts = now
    except KeyboardInterrupt:
        flush_batch(s3_client, buffer)
        consumer.commit()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
