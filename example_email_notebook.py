"""
This is an example DAG which uses Papermill Operator, Python Operator, 
and Email Operator to produce and email reports to various users.
First, the Papermill operator executes a templated notebook,
then the Python operator runs nbconvert to produce an output artifact from
that templated notebook as a pdf. Finally, an email is crafted and sent
with the output pdf attached.
"""

from datetime import datetime, timedelta

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
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

    bash_run_this = BashOperator(
        task_id='run_this',
        bash_command='echo 1',
    )

    bash_then_run_this = BashOperator(
        task_id='run_this',
        bash_command='echo 2',
    )
    bash_run_this >> bash_then_run_this