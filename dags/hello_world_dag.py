# https://medium.com/international-school-of-ai-data-science/hello-world-using-apache-airflow-792947431455
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def helloWorld():
    print('Hello World')

with DAG(dag_id="hello_world_dag",
         start_date=datetime(2021,1,1),
         schedule="@hourly",
         catchup=False) as dag:
    
        task1 = PythonOperator(
                task_id="hello_world",
                python_callable=helloWorld)

        task2 = BashOperator(
                task_id="pwd",
                bash_command="pwd")

task1 >> task2