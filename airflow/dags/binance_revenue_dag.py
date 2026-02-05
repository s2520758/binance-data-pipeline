import pathlib
import sys
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator  


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
PRODUCER_PATH = PROJECT_ROOT / "producer"


if str(PRODUCER_PATH) not in sys.path:
    sys.path.append(str(PRODUCER_PATH))


try:
    from ingest_binance_last_3_days import ingest_last_3_days
except ImportError:
    print(f"Warning: Could not import ingestion script from {PRODUCER_PATH}")
    
    def ingest_last_3_days(end_date_str, days):
        print("Mock ingestion")


def task_ingest_last_3_days(**context):

    logical_date = context["logical_date"].date()
    end_date_str = logical_date.isoformat()
    
    print(f"Starting ingestion for 3 days ending at {end_date_str}...")
   
    ingest_last_3_days(end_date_str, days=3)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="binance_revenue_pipeline",  # DAG ID
    default_args=default_args,
   
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=['dbt', 'binance', 'resume_project'], 
) as dag:

   
    ingest_task = PythonOperator(
        task_id="ingest_last_3_days",
        python_callable=task_ingest_last_3_days,
        provide_context=True,
    )

    
    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command="""
            dbt run \
            --project-dir /opt/airflow/dbt_project \
            --profiles-dir /opt/airflow/dbt_project
        """
    )


    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command="""
            dbt test \
            --project-dir /opt/airflow/dbt_project \
            --profiles-dir /opt/airflow/dbt_project
        """
    )


    ingest_task >> dbt_run_task >> dbt_test_task