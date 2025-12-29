from datetime import date, datetime
import os

import pandas as pd
from google.cloud import bigquery


def main():
    project = os.getenv("BQ_PROJECT", "project-fa672459-e6ce-4619-a6e")
    dataset = os.getenv("BQ_DATASET", "binance_revenue")
    table = os.getenv("BQ_TABLE", "silver_trade_event")

    client = bigquery.Client(project=project)

    df = pd.DataFrame(
        [
            {
                "event_date": date(2025, 12, 28),
                "symbol": "BTCUSDT",
                "trade_id": 1,
                "event_ts": datetime.utcnow(),
                "trade_ts": datetime.utcnow(),
                "price": 12345.67,
                "quantity": 0.001,
                "notional": 12.34567,
                "is_buyer_maker": True,
            }
        ]
    )

    table_id = f"{project}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


if __name__ == "__main__":
    main()
