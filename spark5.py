from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection("minio_s3")

with DAG(
    dag_id="spark_submit_test_1",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_test = SparkSubmitOperator(
        task_id="spark_test",
        application="s3a://spark-jobs/job.py",
        conn_id="spark_standalone",
        deploy_mode="client",
        name="airflow-spark-test",
        verbose=True,

        # This tells Airflow where to get S3 credentials from
        aws_conn_id="minio_s3",

        conf={
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": conn.login,
        "spark.hadoop.fs.s3a.secret.key": conn.password,
        "spark.hadoop.fs.s3a.path.style.access": "true",
    },
    )
