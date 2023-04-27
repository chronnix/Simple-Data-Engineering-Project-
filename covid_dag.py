from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from covid_etl import * 

default_args = {
    'owner': 'postgres',
    'depends_on_past': 'False',
    'start_date': datetime(2023, 4, 26),
    'email': ['tnyoni650@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'covid_dag',
    default_args=default_args,
    description='Our DAG for the Pandemic Analytics Pipeline',
    schedule_interval=None
)

#tasks
task_1 = PythonOperator(
    task_id = 'extract_population_data',
    python_callable=transform_population_data,
    dag=dag
)

task_2 = PythonOperator(
    task_id = 'extract_education_data',
    python_callable=transform_education_data,
    dag=dag
)

task_3 = PythonOperator(
    task_id = 'extract_employment_data',
    python_callable=transform_employment_data,
    dag=dag
)

task_4 = PythonOperator(
    task_id = 'extract_poverty_data',
    python_callable=transform_poverty_data,
    dag=dag
)

task_5 = PythonOperator(
    task_id = 'transform_location_dimension_table',
    python_callable=dim_location,
    dag=dag
)

task_6 = PythonOperator(
    task_id = 'transform_population_dimension_table',
    python_callable=dim_population,
    dag=dag
)

task_7 = PythonOperator(
    task_id = 'transform_fact_table',
    python_callable=fact_table,
    dag=dag 
)

task_8 = PythonOperator(
    task_id = 'load_data_into_the_data_base',
    python_callable=final_stage,
    dag=dag
)

task_1, task_2, task_3, task_4 >> [task_5, task_6, task_7]>>task_8