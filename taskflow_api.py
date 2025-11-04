from airflow.decorators import dag,task
from datetime import datetime,timedelta

# me to taskflow_api den xreiazetai na kanouyme xcoms pull kai push. ta kanei automata. epishs den xreiazetai na oriizouyme th seira p tha
# ektelestoun me >> apla na orisouyme pws vrisketai kathe timh p xreiazetai kai kanei  automata tis ejarthseis

default_args = { # to default_args einai ena dicitonary edw mporwi na exei opoiodhpote onoma
    'owner': 'kat2',
    'retries': 5,
    'retry_delay': timedelta(seconds=5)
}

@dag(dag_id ='tasklow_api_v02',default_args=default_args,description='this is our first dag',
start_date =datetime(2025,6,1),schedule_interval='@daily')
def hello_world_etl():
    
    @task(multiple_outputs=True) # multiple_outputs=True auto to kanw wste na mporei h get_name na epistrefei polles ejodous ara me first k last name
    def get_name():
        return {"firstname":"jerry","lastname":"fridaman"}
        
    
    @task()
    def get_age():
        return 19
        

    @task()
    def greet(firstname,lastname,age):
        print(f"Hello {firstname} {lastname} and {age} years old ")

    name_dict = get_name()
    age = get_age()
    greet(firstname=name_dict["firstname"],lastname=name_dict["lastname"],age=age)

greet_dag = hello_world_etl() # auto einai leei ena instance tou dag mas



