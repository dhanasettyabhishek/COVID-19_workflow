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
from src.transform import TransformData
from src.load import LoadData


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


def delete_file(ds, **kwargs):
    try:
        os.remove("dataFiles")
        os.remove("cleaned_datasets")
        dir_path = os.path.dirname(os.path.realpath(__file__))
        cwd = os.getcwd()
        print(dir_path)
        print(cwd)
    except OSError:
        pass

transform_data_file_1 = PythonOperator(
    task_id="transform_data_file_1",
    provide_context=True,
    python_callable=TransformData.read_file1,
    dag=dag,
    retries=0
)

transform_data_file_2 = PythonOperator(
    task_id="transform_data_file_2",
    provide_context=True,
    python_callable=TransformData.read_file2,
    dag=dag,
    retries=0
)

transform_data_file_3 = PythonOperator(
    task_id="transform_data_file_3",
    provide_context=True,
    python_callable=TransformData.read_file3,
    dag=dag,
    retries=0
)

transform_data_file_4 = PythonOperator(
    task_id="transform_data_file_4",
    provide_context=True,
    python_callable=TransformData.read_file4,
    dag=dag,
    retries=0
)

transform_data_file_5 = PythonOperator(
    task_id="transform_data_file_5",
    provide_context=True,
    python_callable=TransformData.read_file5,
    dag=dag,
    retries=0
)

transform_data_file_6 = PythonOperator(
    task_id="transform_data_file_6",
    provide_context=True,
    python_callable=TransformData.read_file6,
    dag=dag,
    retries=0
)

load_data1 = PythonOperator(
    task_id="load_data1",
    provide_context=True,
    python_callable=LoadData.load_data1,
    dag=dag,
    retries=0
)

load_data2 = PythonOperator(
    task_id="load_data2",
    provide_context=True,
    python_callable=LoadData.load_data2,
    dag=dag,
    retries=0
)

load_data3 = PythonOperator(
    task_id="load_data3",
    provide_context=True,
    python_callable=LoadData.load_data3,
    dag=dag,
    retries=0
)

load_data4 = PythonOperator(
    task_id="load_data4",
    provide_context=True,
    python_callable=LoadData.load_data4,
    dag=dag,
    retries=0
)

load_data5 = PythonOperator(
    task_id="load_data5",
    provide_context=True,
    python_callable=LoadData.load_data5,
    dag=dag,
    retries=0
)

load_data6 = PythonOperator(
    task_id="load_data6",
    provide_context=True,
    python_callable=LoadData.load_data6,
    dag=dag,
    retries=0
)

load_start_date = PythonOperator(
    task_id="load_start_date",
    provide_context=True,
    python_callable=LoadData.start,
    dag=dag,
    retries=0
)

load_end_date = PythonOperator(
    task_id="load_end_date",
    provide_context=True,
    python_callable=LoadData.end,
    dag=dag,
    retries=0
)

load_sex = PythonOperator(
    task_id="load_sex",
    provide_context=True,
    python_callable=LoadData.sex,
    dag=dag,
    retries=0
)

load_race = PythonOperator(
    task_id="load_race",
    provide_context=True,
    python_callable=LoadData.race,
    dag=dag,
    retries=0
)

load_place_of_death = PythonOperator(
    task_id="load_place_of_death",
    provide_context=True,
    python_callable=LoadData.place_of_death,
    dag=dag,
    retries=0
)

load_age_groups = PythonOperator(
    task_id="load_age_groups",
    provide_context=True,
    python_callable=LoadData.age_groups,
    dag=dag,
    retries=0
)

load_states = PythonOperator(
    task_id="load_states",
    provide_context=True,
    python_callable=LoadData.states,
    dag=dag,
    retries=0
)

delete_files_from_local = PythonOperator(
    task_id="delete_files",
    provide_context=True,
    python_callable=delete_file,
    dag=dag,
    retries=0
)

end = DummyOperator(task_id="End", dag=dag)

# Assigning path to tasks

start >> filter_covid_data >> download_data_files

download_data_files >> transform_data_file_1

download_data_files >> transform_data_file_2

download_data_files >> transform_data_file_3

download_data_files >> transform_data_file_4

download_data_files >> transform_data_file_5

download_data_files >> transform_data_file_6

transform_data_file_1 >> load_data1

transform_data_file_2 >> load_data2

transform_data_file_3 >> load_data3
transform_data_file_3 >> load_states

transform_data_file_4 >> load_data4
transform_data_file_4 >> load_age_groups
transform_data_file_4 >> load_sex

transform_data_file_5 >> load_data5
transform_data_file_5 >> load_place_of_death
transform_data_file_5 >> load_start_date
transform_data_file_5 >> load_end_date

transform_data_file_6 >> load_data6
transform_data_file_6 >> load_race

load_data1 >> delete_files_from_local

load_data2 >> delete_files_from_local

load_data3 >> delete_files_from_local
load_states >> delete_files_from_local

load_data4 >> delete_files_from_local
load_age_groups >> delete_files_from_local
load_sex >> delete_files_from_local

load_data5 >> delete_files_from_local
load_place_of_death >> delete_files_from_local
load_start_date >> delete_files_from_local
load_end_date >> delete_files_from_local

load_data6 >> delete_files_from_local
load_race >> delete_files_from_local

delete_files_from_local>> end