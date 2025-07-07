import sys
import os
import boto3
import re 


sys.path.append('/opt/airflow')


from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.operators.python import PythonOperator
from scripts.sentiment_analysis import analyze_sentiment 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from scripts.transform_retail import clean_online_retail
from airflow.providers.postgres.hooks.postgres import PostgresHook


import pandas as pd
import subprocess
from datetime import datetime

BUCKET_NAME = 'etl-pipeline-1-airflow'
LOCAL_DATA_PATH = '/opt/airflow/data/'
TEMP_DOWNLOAD_PATH = '/tmp/movie_review.csv'
TEMP_OUTPUT_PATH = '/tmp/movie_review_sentiment.csv'
RETAIL_CSV_PATH = '/opt/airflow/data/OnlineRetail.csv'
DUCKDB_FILE='tmp/warehouse.duckdb'






default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1
}

def get_partition_path(base_prefix: str) -> str:
    now = datetime.utcnow()
    return f"{base_prefix}/year{now.year}/month={now.month:02}/day={now.day:02}/hour={now.hour:02}/"




with DAG(
    dag_id = 'upload_user_analytics_to_s3',
    default_args=default_args,
    description='Upload OnlineRetail and movie-review data to S3',
    schedule_interval=None,
    catchup=False
) as dag:

    def upload_files_to_s3():
        s3 = S3Hook(aws_conn_id='aws_default')
        # current_date_time = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')

        # s3_prefix = f"{current_date_time}/"
        uploaded_key = []

        for file_name in os.listdir(LOCAL_DATA_PATH):
            local_file_path = os.path.join(LOCAL_DATA_PATH, file_name)

            if not os.path.isfile(local_file_path):
                continue

            if file_name.lower().startswith('movie'):
                s3_prefix = get_partition_path("raw/movie_review")
            elif file_name.lower().startswith('online'):
                s3_prefix = get_partition_path("raw/online_retail")
            else:
                s3_prefix = get_partition_path("raw/misc")
            

            s3_key = f"{s3_prefix}{file_name}"


            s3.load_file(
                filename=local_file_path,
                bucket_name=BUCKET_NAME,
                key=s3_key,
                replace=True,
            )
            uploaded_key.append(s3_key)

        print(f"Uploaded {file_name} to s3://{BUCKET_NAME}/{s3_key}")


        return uploaded_key
            


    upload_task = PythonOperator(
        task_id = 'upload_files_s3',
        python_callable=upload_files_to_s3
    )


    def download_file_from_s3(**context):
        s3 = S3Hook(aws_conn_id='aws_default')

        uploaded_keys = context['ti'].xcom_pull(task_ids='upload_files_s3')

        if not uploaded_keys:
            raise ValueError("No files were uploaded to S3.")

        matching_keys = [key for key in uploaded_keys if key.endswith('movie_review.csv')]

        if not matching_keys:
            raise ValueError("No matching keys found for 'movie_review.csv'.")

        selected_key = matching_keys[0]  # Get the first matching key

        # raw_key = 'movie_review.csv'
        s3.get_key(
            key=selected_key,
            bucket_name=BUCKET_NAME
        ).download_file(TEMP_DOWNLOAD_PATH)
        print(f"Downloaded {selected_key} to {TEMP_DOWNLOAD_PATH}")

    download_review_task = PythonOperator(
        task_id='download_review_file_from_s3',
        python_callable=download_file_from_s3,
        provide_context=True
    )


    def run_sentiment_analysis():
        analyze_sentiment(TEMP_DOWNLOAD_PATH, TEMP_OUTPUT_PATH)
        print(f"Sentiment analysis completed. Results saved to {TEMP_OUTPUT_PATH}")

    sentiment_analysis_task = PythonOperator(
        task_id='run_sentiment_analysis',
        python_callable=run_sentiment_analysis
    )

    def upload_sentiment_results_to_s3(**context):
        s3 = S3Hook(aws_conn_id='aws_default')

        s3_prefix = get_partition_path("processed/movie_review_sentiment")

        results_key = f"{s3_prefix}movie_review_sentiment.csv"
        # current_date_time = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')

        # results_key = f"{current_date_time}/movie_review_sentiment.csv"

        s3.load_file(
            filename=TEMP_OUTPUT_PATH,
            bucket_name=BUCKET_NAME,
            key=results_key,
            replace=True,   
        )

        print(f"Uploaded sentiment analysis results to s3://{BUCKET_NAME}/{results_key}")
        return results_key

    upload_results_task = PythonOperator(
        task_id='upload_sentiment_results_to_s3',
        python_callable=upload_sentiment_results_to_s3,
        provide_context=True
    )


    def clean_and_upload_retail_to_s3():

        s3=S3Hook(aws_conn_id='aws_default')

        local_cleaned_path = '/tmp/online_retail_cleaned.csv'

        subprocess.run(
            ['python3', '/opt/airflow/scripts/transform_retail.py'],
            check=True
        )
        

        s3_key = get_partition_path("processed/online_retail_cleaned/") + 'online_retail_cleaned.csv'

        s3.load_file(
            filename=local_cleaned_path,
            bucket_name=BUCKET_NAME,
            key=s3_key,
            replace=True
        )
        
        
        print(f"Uploaded cleaned online retail data to s3://{BUCKET_NAME}/{s3_key}")

        return s3_key


    clean_upload_retail_task = PythonOperator(
        task_id='clean_and_upload_retail_to_s3',
        python_callable=clean_and_upload_retail_to_s3
    )

    

    def load_csv_to_redshift(s3_key: str, table_name: str):
        redshift = PostgresHook(postgres_conn_id='redshift_default')
        s3_path = f's3://{BUCKET_NAME}/{s3_key}'

        iam_role = 'arn:aws:iam::287216649825:role/RedshiftServerlessAccessRole'

        copy_sql = f"""
        COPY {table_name}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS CSV
        DELIMITER ','
        IGNOREHEADER 1
        TIMEFORMAT 'auto'
        REGION  'us-east-1';
        """

        redshift.run(copy_sql)
        print(f"Data loaded into Redshift table {table_name} from {s3_path}")


    def load_movie_review_sentiment_to_redshift(**context):

        s3_key = context['ti'].xcom_pull(task_ids='upload_sentiment_results_to_s3')
        if not s3_key:
            raise ValueError("No S3 key found for sentiment analysis results.")
        load_csv_to_redshift(s3_key, 'movie_review_sentiment')


    def load_online_retail_cleaned_to_redshift(**context):
        s3_key = get_partition_path("processed/online_retail_cleaned/") + 'online_retail_cleaned.csv'
        load_csv_to_redshift(s3_key, 'online_retail_cleaned')

    load_sentiment_redshift_task = PythonOperator(
        task_id='load_movie_review_sentiment_to_redshift',
        python_callable=load_movie_review_sentiment_to_redshift,
        provide_context=True
    )

    load_retail_redshift_task = PythonOperator(
        task_id='load_online_retail_cleaned_to_redshift',
        python_callable=load_online_retail_cleaned_to_redshift,
        provide_context=True
    )



upload_task >> download_review_task >> sentiment_analysis_task >> upload_results_task >> load_sentiment_redshift_task
upload_task >> clean_upload_retail_task >> load_retail_redshift_task
# upload_task >> load_raw_retail_task_to_duckdb
# upload_task >> load_movie_review_raw_task_to_duckdb

