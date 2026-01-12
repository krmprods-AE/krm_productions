from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

JOB_LOCAL_DIR = "/opt/spark/jobs"
JOB_LOCAL_PATH = f"{JOB_LOCAL_DIR}/job.py"
S3_BUCKET = "spark-jobs"
S3_KEY = "job.py"


def fetch_spark_job():
    os.makedirs(JOB_LOCAL_DIR, exist_ok=True)

    hook = S3Hook(aws_conn_id="minio_s3")
    hook.download_file(
        key=S3_KEY,
        bucket_name=S3_BUCKET,
        local_path="/opt/spark/jobs/job.py",
        preserve_file_name=True,
    )


with DAG(
    dag_id="spark_submit_with_s3_fetch",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "minio"],
) as dag:

    fetch_job = PythonOperator(
        task_id="fetch_spark_job",
        python_callable=fetch_spark_job,
    )

    spark_submit = SparkSubmitOperator(
        task_id="run_spark_job",
        application=JOB_LOCAL_PATH,
        conn_id="spark_standalone",
        deploy_mode="client",
        name="airflow-spark-job",
        verbose=True,
    )

    fetch_job >> spark_submit
