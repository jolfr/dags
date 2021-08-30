"""
This is an example DAG which uses Papermill Operator, Python Operator, 
and Email Operator to produce and email reports to various users.
First, the Papermill operator executes a templated notebook,
then the Python operator runs nbconvert to produce an output artifact from
that templated notebook as a pdf. Finally, an email is crafted and sent
with the output pdf attached.
"""

from datetime import datetime, timedelta
import os

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
# [END import_module]


# [START instantiate_dag]
default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': "1",
    'tags': ['example', 'notebook'],
}

with DAG(
    'email_notebook',
    default_args=default_args,
    description='example to execute and email notebook to stakeholders',
    schedule_interval='@daily',
    start_date=datetime(2021, 8, 28),
    catchup=False,
) as dag:

    first_task = PapermillOperator(
        task_id='first_task',
        input_nb="{}/example_notebook.ipynb".format(os.getcwd()),
        output_nb="/tmp/out-{{ execution_date }}.ipynb",
        parameters={"msgs": "Ran from Airflow at {{ execution_date }}!"},
    )

    second_task = DummyOperator(
        task_id='second_task',
    )

    third_task = DummyOperator(
        task_id='third_task',
    )

    first_task >> second_task >> third_task