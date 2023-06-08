from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator


def lilyCommand(tenant: str, command: str, dnaEntityType: str = None, arguments: str = None) -> BashOperator:
    if dnaEntityType is None:
        task_id = command
        args = ""
    else:
        task_id = dnaEntityType + "-" + command
        args = "--dna-entity-type " + dnaEntityType

    if not arguments is None:
        args = args + " " + arguments

    return BashOperator(
        task_id=task_id,
        bash_command="lily " + command + " -conf /tmp/" + tenant + "/lily-site.xml " + args,
    )


# ugly hack, works only if you have one worker
def copyLilySite(tenant: str) -> BashOperator:
    return BashOperator(
        task_id="copy-lily-site",
        bash_command="rm -f /tmp/" + tenant + "/lily-site.xml && mkdir -p /tmp/" + tenant + " && lily fs -get /lily/tenant/company1/system-config/lily-site.xml /tmp/" + tenant + "/lily-site.xml",
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
        description="Company1-daily",
        schedule=None,
        start_date=datetime(2023, 6, 6),
        catchup=False,
        tags=["company1"],
) as dag:

    tenant = "company1"

    copyLilySite = copyLilySite(tenant)
    dataframeExecute = lilyCommand(tenant,
                                   "data-frame-execute",
                                   "{{ dag_run.conf['dnaEntityType'] }}",
                                   "--cr service:svc:69115adf-97c2-4bb4-837f-3e357ffd230d " +
                                   "--name {{ dag_run.conf['dataframeName'] }}")

    copyLilySite >> dataframeExecute

