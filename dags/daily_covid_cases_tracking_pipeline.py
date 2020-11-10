# import libraries
from datetime import timedelta
import os

# Airflow
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Python files
from src.get_api_data import GetData
from src.download_files import DownloadData
from src.validate_downloaded_data import Validations
from src.data_preprocessing import DataPreprocessing
from src.load_data_to_postgres import LoadData
from src.read_data_from_postgres import ReadData

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
    'daily_covid_cases_tracking_pipeline',
    default_args=default_args,
    description='Data Engineering Project',
    schedule_interval='@daily'
)


# Creating Tasks

start = DummyOperator(task_id="Start", dag=dag)

filter_covid_data = PythonOperator(
    task_id="filter_covid_data",
    provide_context=True,
    python_callable=GetData.filter_data,
    dag=dag,
    retries=0
)

download_files = PythonOperator(
    task_id="download_files",
    provide_context=True,
    python_callable=DownloadData.download_files,
    dag=dag,
    retries=0
)

validating_downloaded_data = PythonOperator(
    task_id="validating_downloaded_data",
    provide_context=True,
    python_callable=Validations.validate_downloaded_data,
    dag=dag,
    retries=0
)

data_preprocessing_ = []
for key, value in DataPreprocessing.__dict__.items():
    if type(value) == staticmethod:
        data_preprocessing_.append(key)

preprocessing = []
for dp in data_preprocessing_:
    name = "DataPreprocessing." + dp
    load_ = "load_" + str(dp)
    dp = PythonOperator(
        task_id=dp,
        provide_context=True,
        python_callable=eval(name),
        dag=dag,
        retries=0
    )
    preprocessing.append(dp)

load_data_to_postgres = []
for x, y in LoadData.__dict__.items():
    if type(y) == staticmethod:
        load_data_to_postgres.append(x)

data_to_postgres = []
for ld in load_data_to_postgres:
    name = "LoadData." + ld
    ld = PythonOperator(
        task_id=ld,
        provide_context=True,
        python_callable=eval(name),
        dag=dag,
        retries=0
    )
    data_to_postgres.append(ld)


def delete_file(ds, **kwargs):
    try:
        os.remove("dataFiles")
        os.remove("cleaned_datasets")
        os.remove("downloadedData")
    except OSError:
        pass


delete_files_from_local = PythonOperator(
    task_id="delete_files",
    provide_context=True,
    python_callable=delete_file,
    dag=dag,
    retries=0
)

read_data_from_postgres = PythonOperator(
    task_id="read_data_from_postgres",
    provide_context=True,
    python_callable=ReadData.read_data_from_postgres,
    dag=dag,
    retries=0
)

end = DummyOperator(task_id="End", dag=dag)

# Assigning path to tasks

start >> filter_covid_data >> download_files >> validating_downloaded_data

for pp in preprocessing:
    validating_downloaded_data >> pp

for i in range(len(preprocessing)):
    preprocessing[i] >> data_to_postgres[i]

# Dependencies
preprocessing[2] >> data_to_postgres[-1]

preprocessing[3] >> data_to_postgres[-3]
preprocessing[3] >> data_to_postgres[-2]

preprocessing[4] >> data_to_postgres[-4]
preprocessing[4] >> data_to_postgres[-5]
preprocessing[4] >> data_to_postgres[-6]

preprocessing[5] >> data_to_postgres[6]

for dp in data_to_postgres:
    dp >> delete_files_from_local

delete_files_from_local >> read_data_from_postgres >> end
