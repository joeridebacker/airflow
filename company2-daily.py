from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator


def lilyCommand(tenant: str, command: str, dnaEntityType: str = None) -> BashOperator:
    if dnaEntityType is None:
        task_id = command
        args = ""
    else:
        task_id = dnaEntityType + "-" + command
        args = "--dna-entity-type " + dnaEntityType

    return BashOperator(
        task_id=task_id,
        bash_command="/opt/ngdata/scripts/lily_wrapper.sh " + tenant + " " + command + " " + args,
    )


with DAG(
        "Company2-daily",
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
        description="Company2-daily",
        schedule=timedelta(days=1),
        start_date=datetime(2023, 6, 6),
        catchup=False,
        tags=["company1"],
) as dag:

    tenant = "company2"

    itxBatchAugmentation = lilyCommand(tenant, "itx-batch-augmentation")

    tasks = []
    for dnaEntityType in ["CUSTOMER", "CANDIDATE", "DEVICE"]:
        dnaItxBatchCalc = lilyCommand(tenant, "dna-itx-batch-calc", dnaEntityType)
        dnaEntityBatchCalc = lilyCommand(tenant, "dna-entity-batch-calc", dnaEntityType)
        itxViewBatchCalc = lilyCommand(tenant, "itx-view-batch-calc", dnaEntityType)
        setMembershipCalc = lilyCommand(tenant, "set-membership-calc", dnaEntityType)
        dnaSetBatchCalc = lilyCommand(tenant, "dna-set-batch-calc", dnaEntityType)
        dnaItxBatchCalc >> dnaEntityBatchCalc >> itxViewBatchCalc >> setMembershipCalc >> dnaSetBatchCalc
        tasks.append(dnaItxBatchCalc)

    itxBatchAugmentation >> tasks

# lily itx-batch-augmentation -conf /lily/tenant/company1/system-config/lily-site.xml
