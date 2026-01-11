from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection("minio_s3")

spark_test = SparkSubmitOperator(
    task_id="spark_test",
    application="s3a://spark-jobs/job.py",
    conn_id="spark_standalone",
    deploy_mode="client",
    name="airflow-spark-test",
    verbose=True,
    conf={
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": conn.login,
        "spark.hadoop.fs.s3a.secret.key": conn.password,
        "spark.hadoop.fs.s3a.path.style.access": "true",
    },
)
