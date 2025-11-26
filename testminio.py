from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

def test_minio():
    hook = S3Hook(aws_conn_id="minio_s3")
    print("Buckets:", hook.list_buckets())
    print("Files:", hook.list_keys(bucket_name="example"))

with DAG("test_minio_conn", start_date=datetime(2024,1,1), schedule=None, catchup=False):
    PythonOperator(
        task_id="test_minio",
        python_callable=test_minio
    )
