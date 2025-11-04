from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

default_args = { # to default_args einai ena dicitonary edw mporwi na exei opoiodhpote onoma
    'owner': 'kat2',
    'retries': 5,
    'retry_delay': timedelta(seconds=5)
}



with DAG(
    dag_id ='dag_from_laptop_change_from_desktop',
    default_args=default_args,
    description='this is our first python operator dag',
    start_date =datetime(2025,10,25),
    schedule_interval='@daily',
    #catchup=True # to cathup shmainei na ekteleitai oses fores eprepe na ektelestei me bash to start date p tou xw orisei mexri shmera analoga to 
    #schedule interval

# PROSOXH PROSOXH TO cathup BY DEFAULT EINAI TRUE

) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello  world"
    )
    task2 = BashOperator(
      task_id='second_task',
      bash_command="echo hello  world 2nd task after task1"
    )
    task3 = BashOperator(
      task_id='3rd_task',
      bash_command="echo hello  world i am the third"
    )

    task1 >> task2 >> task3