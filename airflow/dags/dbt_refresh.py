from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_refresh',
    description='Refresh dbt staging and marts every 30 minutes',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/dbt && dbt run --profiles-dir /opt/dbt --target docker',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/dbt && dbt test --profiles-dir /opt/dbt --target docker',
    )

    dbt_run >> dbt_test