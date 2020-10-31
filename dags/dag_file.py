# import libraries
from datetime import timedelta

# Airflow
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Python files
from src.get_api_data import GetData

# default arguments
default_args = {
    'owner': 'Abhishek',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'email': ['dhanasettyabhishek@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Creating Dag
dag = DAG(
    'covid_workflow',
    default_args=default_args,
    description='Data Engineering Project',
    schedule_interval='@daily'
)

# Creating Tasks

start = DummyOperator(task_id="Start", dag=dag)

filter_covid_data = PythonOperator(
    task_id="filter_covid_data",
    provide_context=True,
    python_callable=GetData.download_url,
    dag=dag,
    retries=0
)

download_data_files = PythonOperator(
    task_id="download_data_files",
    provide_context=True,
    python_callable=GetData.download_data,
    dag=dag,
    retries=0
)

end = DummyOperator(task_id="End", dag=dag)

# Assigning path to tasks

start >> filter_covid_data >> download_data_files >> end
