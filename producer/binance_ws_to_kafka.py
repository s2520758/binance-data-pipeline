import json
import os
import signal
import sys
import threading
import time
from typing import List

from confluent_kafka import Producer
from websocket import WebSocketApp

BINANCE_BASE_URL = "wss://stream.binance.com:9443/stream"
SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt"]
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "binance_trades_raw")

producer = None
ws_app = None
running = True


def create_kafka_producer() -> Producer:
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "binance-trade-producer",
        "enable.idempotence": True,
    }
    return Producer(config)


def delivery_report(err, msg):
    if err is not None:
        return


def send_to_kafka(key: str, value: dict):
    payload = json.dumps(value, separators=(",", ":"))
    producer.produce(
        topic=KAFKA_TOPIC,
        key=key.encode("utf-8"),
        value=payload.encode("utf-8"),
        callback=delivery_report,
    )


def on_open(ws):
    streams = [f"{s}@trade" for s in SYMBOLS]
    params = {
        "method": "SUBSCRIBE",
        "params": streams,
        "id": 1,
    }
    ws.send(json.dumps(params))


def on_message(ws, message: str):
    try:
        data = json.loads(message)
        if "stream" not in data or "data" not in data:
            return
        stream = data["stream"]
        payload = data["data"]
        if payload.get("e") != "trade":
            return
        symbol = payload.get("s", "").lower()
        key = symbol or "unknown"
        record = {
            "stream": stream,
            "event_type": payload.get("e"),
            "event_time": payload.get("E"),
            "symbol": payload.get("s"),
            "trade_id": payload.get("t"),
            "price": payload.get("p"),
            "quantity": payload.get("q"),
            "buyer_order_id": payload.get("b"),
            "seller_order_id": payload.get("a"),
            "trade_time": payload.get("T"),
            "is_buyer_maker": payload.get("m"),
            "ignore": payload.get("M"),
            "ingest_time": int(time.time() * 1000),
        }
        send_to_kafka(key, record)
    except Exception:
        return


def on_error(ws, error):
    time.sleep(1)


def on_close(ws, status_code, msg):
    time.sleep(1)


def build_ws_url(symbols: List[str]) -> str:
    streams = "/".join([f"{s}@trade" for s in symbols])
    return f"{BINANCE_BASE_URL}?streams={streams}"


def run_ws():
    global ws_app
    url = build_ws_url(SYMBOLS)
    ws_app = WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws_app.run_forever(ping_interval=20, ping_timeout=10)


def shutdown(signum, frame):
    global running
    running = False
    if ws_app is not None:
        try:
            ws_app.close()
        except Exception:
            pass
    if producer is not None:
        try:
            producer.flush(5)
        except Exception:
            pass
    sys.exit(0)


def main():
    global producer
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    producer = create_kafka_producer()
    thread = threading.Thread(target=run_ws, daemon=True)
    thread.start()
    while running:
        producer.poll(0.1)
        time.sleep(0.1)


if __name__ == "__main__":
    main()
