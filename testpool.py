from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
        "testpools",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description="testpools",
        schedule=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["testpool"],
) as dag:


    t1 = BashOperator(
        task_id="t1-1",
        bash_command="sleep 10",
        pool_slots=1
    )
    t2 = BashOperator(
        task_id="t2-2",
        bash_command="sleep 10",
        pool_slots=2
    )
    t3 = BashOperator(
        task_id="t3-4",
        bash_command="sleep 10",
        pool_slots=4
    )
    t4 = BashOperator(
        task_id="t4-8",
        bash_command="sleep 10",
        pool_slots=8
    )
    t5 = BashOperator(
        task_id="t5-16",
        bash_command="sleep 10",
        pool_slots=16
    )
    t6 = BashOperator(
        task_id="t6-32",
        bash_command="sleep 10",
        pool_slots=32
    )
    t7 = BashOperator(
        task_id="t7-64",
        bash_command="sleep 10",
        pool_slots=64
    )
    t8 = BashOperator(
        task_id="t8-128",
        bash_command="sleep 10",
        pool_slots=128
    )



    t1 >> [t2, t3, t4, t5, t6, t7, t8]