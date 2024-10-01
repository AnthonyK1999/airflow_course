from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'tutorial',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    t1.doc_md = dedent(
        """
        ####Task Documentation
        I am lazy to do this documentation
        """
    )

    dag.doc_md = __doc__
    dag.doc_md = """
    This is a documentation placed anywhere
    """

    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ds }"
            echo "{{ macros.ds_add(ds, 7)}}"
        {% endfor %}
        """
    )

    def print_context(ds, **kwargs):
	"""Пример PythonOperator"""
	print(kwargs)
	print(ds)
	return 'Whatever you return gets printed in the logs'

    t4 = PythonOperator(
	task_id='print_the_context',
	python_callable=print_context,
    )


    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )

    t1 >> [t2, t3] >> t4
