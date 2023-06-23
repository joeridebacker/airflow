from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator


def lilyCommand(tenant: str, command: str, dnaEntityType: str = None, arguments: str = None) -> BashOperator:
    task_id = command
    if dnaEntityType is None:
        args = ""
    else:
        args = "--dna-entity-type " + dnaEntityType

    if arguments is not None:
        args = args + " " + arguments

    return BashOperator(
        task_id=task_id,
        bash_command="/opt/ngdata/scripts/lily.sh " + tenant + " " + command + " " + args,
    )


with DAG(
        "Company1-dataframe",
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            # 'queue': 'bash_queue',
            # 'pool': 'backfill',
            # 'priority_weight': 10,
            # 'end_date': datetime(2016, 1, 1),
            # 'wait_for_downstream': False,
            # 'sla': timedelta(hours=2),
            # 'execution_timeout': timedelta(seconds=300),
            # 'on_failure_callback': some_function, # or list of functions
            # 'on_success_callback': some_other_function, # or list of functions
            # 'on_retry_callback': another_function, # or list of functions
            # 'sla_miss_callback': yet_another_function, # or list of functions
            # 'trigger_rule': 'all_success'
        },
        description="Company1-dataframe",
        schedule=None,
        start_date=datetime(2023, 6, 6),
        catchup=False,
        tags=["company1"],
) as dag:
    tenant = "company1"

    dataframeExecute = lilyCommand(tenant,
                                   "data-frame-execute",
                                   "{{ dag_run.conf['dna-entity-type'] }}",
                                   "--cr {{ dag_run.conf['credentials'] }} " +
                                   "--name {{ dag_run.conf['dataframe-name'] }}")

# {"dna-entity-type": "CUSTOMER", "dataframe-name": "BasicDataframeAll", "credentials": "service:svc:32164433-0ed6-40b6-8a30-5c0b35ef23cc"}
# {"dna-entity-type": "CUSTOMER", "dataframe-name": "AdvancedDataFrame", "credentials": "service:svc:cc9da7a9-aa1e-4ac6-9573-953e162b57b7"}


