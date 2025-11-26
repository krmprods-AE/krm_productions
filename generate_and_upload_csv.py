from datetime import datetime
import random
import csv
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


BUCKET_NAME = "example"
ROWS = 40   # number of random rows to generate


def generate_random_csv_file():
    """Generate a random telecom CSV with headers and return the local file path."""
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    file_path = tmp_file.name

    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f, delimiter=",") # made it comma-seperated

        # HEADER row
        writer.writerow(["timestamp", "msisdn", "charge", "service"])

        # DATA rows
        for _ in range(ROWS):
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S+01")
            msisdn = random.randint(100000, 999999)
            charge = random.choice([0.15, 0.30, 0.75, 1.50])
            service = random.choice(["SMS", "***"])
            writer.writerow([timestamp, msisdn, charge, service])

    print(f"Generated file with headers: {file_path}")
    return file_path



def upload_to_minio(file_path, **context):
    """Upload the generated CSV to MinIO bucket."""
    hook = S3Hook(aws_conn_id="minio_s3")

    # Choose a unique filename
    filename = f"random_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    hook.load_file(
        filename=file_path,
        bucket_name=BUCKET_NAME,
        key=filename,
        replace=True
    )

    print(f"Uploaded {filename} to bucket '{BUCKET_NAME}'")


with DAG(
    dag_id="generate_and_upload_csv",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/2 * * * *",   # every 2 minutes
    catchup=False,
    tags=["minio", "etl", "csv"],
) as dag:

    generate_csv = PythonOperator(
        task_id="generate_csv",
        python_callable=generate_random_csv_file,
    )

    upload_csv = PythonOperator(
        task_id="upload_csv",
        python_callable=upload_to_minio,
        op_kwargs={"file_path": "{{ ti.xcom_pull(task_ids='generate_csv') }}"},
    )

    generate_csv >> upload_csv
