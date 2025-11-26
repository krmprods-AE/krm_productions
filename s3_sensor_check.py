from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator


BUCKET = "example"


def process_new_file(**context):
    key = context["ti"].xcom_pull(task_ids="wait_for_csv")
    print(f"New CSV detected: {key}")
    # Here you later trigger Spark, transform data, load to DB, etc.


with DAG(
    dag_id="s3_minio_sensor",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/2 * * * *",   # check every 2 minutes
    catchup=False,
    tags=["minio", "sensor", "etl"],
) as dag:

    wait_for_csv = S3KeySensor(
        task_id="wait_for_csv",
        bucket_key="*.csv",     # pattern match
        wildcard_match=True,
        bucket_name=BUCKET,
        aws_conn_id="minio_s3",
        poke_interval=10,       # check every 20 seconds
        timeout=60 * 5,         # give up after 5 minutes
        do_xcom_push=True,  # take the name of the new file found
    )

    process_file = PythonOperator(
        task_id="process_file",
        python_callable=process_new_file,
        provide_context=True,
    )

    wait_for_csv >> process_file
   # Short Answer

# S3KeySensor cannot detect new CSVs,
# and it does NOT push the actual file name into XCom the way you think.
# 
# So NO, your DAG:
# 
# S3KeySensor(... do_xcom_push=True ...)
# 
# 
# WILL NOT reliably tell you the name of new CSV files.
# 
# This is why you always see:
# 
# New CSV detected: None
# 
# ❗ Why S3KeySensor Cannot Do What You Want
# 1️⃣ It only checks whether any matching file exists
# 
# It does not know which file is new.
# 
# 2️⃣ It matches on a wildcard pattern
# 
# bucket_key="*.csv" gives this behavior:
# 
# If ANY .csv exists → sensor instantly succeeds
# 
# If NO .csv exists → sensor waits
# 
# It does NOT track new files
# 
# It does NOT detect file changes
# 
# It does NOT sort files
# 
# It does NOT know timestamps
# 
# It does NOT know versions
# 
# 3️⃣ do_xcom_push=True only pushes a boolean
# 
# This is the key misunderstanding.
# 
# ✔ Some sensors push an object
# ❌ S3KeySensor pushes only True
# 
# From Airflow source code:
# 
# return True  # when condition is satisfied
# 
# 
# So you always get:
# 
# key = True
