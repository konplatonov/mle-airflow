# dags/churn.py
import pendulum
from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.python import PythonOperator
from steps.churn import create_table, extract, transform, load
from steps.messages import send_telegram_success_message, send_telegram_failure_message

with DAG(
    dag_id='prepare_churn_dataset',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    on_failure_callback=send_telegram_failure_message,
    on_success_callback=send_telegram_success_message,
    tags=["ETL"]
) as dag:

    create_table()
    extract_step = PythonOperator(task_id='extract', python_callable=extract)
    transform_step = PythonOperator(task_id='transform', python_callable=transform)
    load_step = PythonOperator(task_id='load', python_callable=load)

    extract_step >> transform_step >> load_step
