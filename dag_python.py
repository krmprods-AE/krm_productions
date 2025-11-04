from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = { # to default_args einai ena dicitonary edw mporwi na exei opoiodhpote onoma
    'owner': 'kat2',
    'retries': 5,
    'retry_delay': timedelta(seconds=5)
}

def greet(ti):# ti einai to task instance kai to bazw  giati to xcoms_pull can only be called by ti
    name1 = ti.xcom_pull(task_ids = 'get_name',key ='first_name') # PROSOXH PROSOXH otan dhlwnw to key prepei na nai mesa se "" h ''
    name2 = ti.xcom_pull(task_ids = 'get_name',key ='last_name')
    age = ti.xcom_pull(task_ids = 'get_age',key ='age')
    print(f"Hello world!!! My name is {name1} {name2} and i am {age} years old")

def get_name(ti):
    ti.xcom_push(key="first_name",value="jerry")
    ti.xcom_push(key="last_name",value="frery")

def get_age(ti):
    ti.xcom_push(key="age",value=20)


with DAG(
    dag_id ='python_dag_v06',
    default_args=default_args,
    description='this is our first dag',
    start_date =datetime(2025,6,1),
    schedule_interval='@daily'
    #catchup=True # to cathup shmainei na ekteleitai oses fores eprepe na ektelestei me bash to start date p tou xw orisei mexri shmera analoga to 
    #schedule interval
) as dag:
    task1 = PythonOperator(
        task_id = 'greet',
        python_callable=greet,
        #op_kwargs = {'age': 20} efoson vazw xcom pull kai push gia thn greet den to xreiazomai auto

    )
    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable = get_name,
    )
    task3 = PythonOperator(
        task_id = 'get_age',
        python_callable = get_age,
    )


    [task2,task3] >> task1

#pws pernaw information apo ena task se ena allo? Me to XCOMS by default every function's return value will be automatically pushed into XCOMS
#ama pas sto admin kai xcoms tha deis to jerry san return_value. epishs tha deis kai returns kai outputs allwn tasks p xame orisei. opote ola apo ta 
#tasks pane ekei. PROSOXH. to maximum size tou XCOMS einai MONO 48kb