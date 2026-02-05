from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'binance_dbt_transformation',
    default_args=default_args,
    description='A dbt pipeline for Binance trades',

    start_date=datetime(2023, 1, 1),
  
    schedule_interval='@hourly',
    catchup=False,
    tags=['dbt', 'binance', 'resume_project'],
) as dag:

   
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='''
            dbt run \
            --project-dir /opt/airflow/dbt_project \
            --profiles-dir /opt/airflow/dbt_project
        '''
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='''
            dbt test \
            --project-dir /opt/airflow/dbt_project \
            --profiles-dir /opt/airflow/dbt_project
        '''
    )

    dbt_run >> dbt_test