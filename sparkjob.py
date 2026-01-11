from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="spark_submit_test",
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
        conf={
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": "mycustomuser",
        "spark.hadoop.fs.s3a.secret.key": "pakekfoeo3030d3*(&&&(*!",
        "spark.hadoop.fs.s3a.path.style.access": "true",
    },

    )
