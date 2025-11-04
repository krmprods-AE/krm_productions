from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

default_args = { # to default_args einai ena dicitonary edw mporwi na exei opoiodhpote onoma
    'owner': 'kat2',
    'retries': 5,
    'retry_delay': timedelta(seconds=5)
}



with DAG(
    dag_id ='cronjobs_vo1',
    default_args=default_args,
    description='this is our first python operator dag',
    start_date =datetime(2025,5,28),
    schedule_interval='0 3 * * Tue-Fri',
    #catchup=False # twra epeidi ekana to cathcup = false den trexei apo prin. omws ama thelw mporw na kanw "backfill" kai na trejw kai gi a prohgoyumenes
    #hmeromhnies. Mpainw sto docker tou scheduler me -it bash kai dinw thn entolh airflow dags backfill -s 2025-05-28 -e 2025-06-05 dag_with_catchup_v002
    # opote ama kanw refresh blepw oti etreje gia tis prohgoumenes hmeromhnies
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo DAG WITH CATHUP"
    )
    task1