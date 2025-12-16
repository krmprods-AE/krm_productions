from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

def test_minio():
    hook = S3Hook(aws_conn_id="minio_s3")

    # List files inside your bucket "example"
    keys = hook.list_keys(bucket_name="test")

    print("Files in bucket:", keys)
    return keys

with DAG(
    dag_id="twra",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
):
    PythonOperator(
        task_id="test_minio",
        python_callable=test_minio,
    )
