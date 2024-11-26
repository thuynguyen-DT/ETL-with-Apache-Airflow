from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import logging

input_file = '/root/airflow_project/web-server-access-log.txt'
extracted_file = '/root/airflow_project/extracted_data.txt'
transform_file = '/root/airflow_project/transformed_data.txt'
output_file = '/root/airflow_project/capitalized_data.txt'

default_args = {
    'owner': 'Thuy',
    'start_date': days_ago(2),
    'email': ['nphuongthuy1408@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_first_python_ETL_dag',
    default_args=default_args,
    description='My first DAG',
    schedule_interval=timedelta(days=1),
)

def download_file():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            with open(input_file, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
        logging.info(f"File downloaded successfully: {input_file}")
    except Exception as e:
        logging.error(f"Error downloading file: {e}")

def extract():
    logging.info('Inside extract')
    try:
        with open(input_file, 'r') as infile, open(extracted_file, 'w') as outfile:
            for line in infile:
                fields = line.split('#')
                if len(fields) >= 4:
                    field_1 = fields[0]
                    field_4 = fields[3]
                    outfile.write(field_1 + "#" + field_4 + "\n")
    except Exception as e:
        logging.error(f"Error during extraction: {e}")

def transform():
    logging.info('Inside transform')
    try:
        with open(extracted_file, 'r') as infile, open(transform_file, 'w') as outfile:
            for line in infile:
                proceed_line = line.upper()
                outfile.write(proceed_line + "\n")
    except Exception as e:
        logging.error(f"Error during transformation: {e}")

def load():
    logging.info('Inside load')
    try:
        with open(transform_file, 'r') as infile, open(output_file, 'w') as outfile:
            for line in infile:
                outfile.write(line + "\n")
    except Exception as e:
        logging.error(f"Error during loading: {e}")

def check():
    logging.info('Inside check')
    try:
        with open(output_file, 'r') as infile:
            for line in infile:
                logging.info(line.strip())
    except Exception as e:
        logging.error(f"Error during check: {e}")

download = PythonOperator(
    task_id='download',
    python_callable=download_file,
    dag=dag,
)

execute_extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

execute_transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

execute_load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

execute_check = PythonOperator(
    task_id='check',
    python_callable=check,
    dag=dag,
)

download >> execute_extract >> execute_transform >> execute_load >> execute_check
