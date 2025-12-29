from datetime import timedelta

import os
import sys
import pathlib
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
PRODUCER_PATH = PROJECT_ROOT / "producer"

if str(PRODUCER_PATH) not in sys.path:
    sys.path.append(str(PRODUCER_PATH))

from transform_bronze_to_silver import main as silver_main
from build_fact_fee_tax import main as fact_main

PROJECT_ID = "project-fa672459-e6ce-4619-a6e"


def run_silver(ds, **context):
    os.environ["S3_BUCKET"] = "ealtime-revenue-binance"
    os.environ["BRONZE_PREFIX"] = "bronze_trade_event"
    os.environ["SILVER_PREFIX"] = "silver_trade_event"
    os.environ["BQ_PROJECT"] = PROJECT_ID
    os.environ["BQ_DATASET"] = "binance_revenue"
    os.environ["BQ_SILVER_TABLE"] = "silver_trade_event"
    silver_main(ds)


def run_fact(ds, **context):
    os.environ["S3_BUCKET"] = "ealtime-revenue-binance"
    os.environ["SILVER_PREFIX"] = "silver_trade_event"
    os.environ["FACT_PREFIX"] = "fact_trade_fee_tax"
    os.environ["FEE_TAX_RULES_PATH"] = "producer/config/fee_tax_rules.csv"
    os.environ["DEFAULT_REGION"] = "EU"
    os.environ["BQ_PROJECT"] = PROJECT_ID
    os.environ["BQ_DATASET"] = "binance_revenue"
    os.environ["BQ_FACT_TABLE"] = "fact_trade_fee_tax"
    fact_main(ds)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="binance_revenue_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 12, 27, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:
    silver_task = PythonOperator(
        task_id="silver_trade_event",
        python_callable=run_silver,
        provide_context=True,
    )

    fact_task = PythonOperator(
        task_id="fact_trade_fee_tax",
        python_callable=run_fact,
        provide_context=True,
    )

    silver_task >> fact_task
