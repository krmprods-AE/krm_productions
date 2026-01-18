from airflow import DAG
#from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.sensors.python import PythonSensor
from datetime import datetime
import boto3
import json

default_args = {
    "owner": "airflow",
    "retries": 1,
}
def parquet_exists():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="mycustomuser",
        aws_secret_access_key="pakekfoeo3030d3*(&&&(*!",
    )

    response = s3.list_objects_v2(
        Bucket="analytics",
        Prefix="joined_orders/"
    )

    if "Contents" not in response:
        return False

    return any(obj["Key"].endswith(".parquet") for obj in response["Contents"])

with DAG(
    dag_id="spark_submit_operator_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["spark_submit_operator","spark","employee3","S3","2_jobs","sensor","wait","pipeline"],
) as dag:

    
    wait_for_parquet = PythonSensor(
    task_id="wait_for_parquet_files",
    python_callable=parquet_exists,
    poke_interval=10,
    timeout=300,
    mode="reschedule",
        )

    spark_job1 = SparkSubmitOperator(
    task_id="spark_job1",
    application="s3a://spark-jobs/app_read_join.py",
    conn_id="spark_standalone",
    deploy_mode="client",
    name="airflow-spark-job",
    verbose=True,
    spark_binary="/opt/spark/bin/spark-submit",
    conf={
        "spark.hadoop.fs.s3a.access.key": "mycustomuser",
        "spark.hadoop.fs.s3a.secret.key": "pakekfoeo3030d3*(&&&(*!",
    },)

    spark_job2 = SparkSubmitOperator(
    task_id="spark_job2",
    application="s3a://spark-jobs/1_csv_write.py",
    conn_id="spark_standalone",
    deploy_mode="client",
    name="airflow-spark-job",
    verbose=True,
    spark_binary="/opt/spark/bin/spark-submit",
    conf={
        "spark.hadoop.fs.s3a.access.key": "mycustomuser",
        "spark.hadoop.fs.s3a.secret.key": "pakekfoeo3030d3*(&&&(*!",
    },
    
)

    spark_job1 >> wait_for_parquet >> spark_job2
