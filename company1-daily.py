from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator


def lilyCommand(tenant: str, command: str, pool_slots: int = 1, dna_entity_type: str = None, memory_requests: str = None,
                memory_limits: str = None) -> BashOperator:
    if dna_entity_type is None:
        task_id = command
        args = ""
    else:
        task_id = dna_entity_type + "-" + command
        args = "--dna-entity-type " + dna_entity_type

    if memory_requests is None:
        memory_requests = "512Mi"
    if memory_limits is None:
        memory_limits = memory_requests

    return BashOperator(
        task_id=task_id,
        pool_slots=pool_slots,
        bash_command="/opt/ngdata/scripts/lily.sh " + tenant + " " + command + " " + args,
        executor_config={
            "KubernetesExecutor": {"request_memory": memory_requests,
                                   "limit_memory": memory_limits}},
    )


with DAG(
        "Company1-daily",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        description="Company1-daily",
        schedule=timedelta(days=1),
        start_date=datetime(2023, 6, 6),
        catchup=False,
        tags=["company1"],
) as dag:

    tenant = "company1"

    itxBatchAugmentation = lilyCommand(tenant, "itx-batch-augmentation", 128)

    tasks = []
    for dnaEntityType in ["CUSTOMER", "CANDIDATE", "DEVICE"]:
        dnaItxBatchCalc = lilyCommand(tenant, "dna-itx-batch-calc", 65, dnaEntityType)
        dnaEntityBatchCalc = lilyCommand(tenant, "dna-entity-batch-calc", 60, dnaEntityType)
        itxViewBatchCalc = lilyCommand(tenant, "itx-view-batch-calc", 2, dnaEntityType)
        setMembershipCalc = lilyCommand(tenant, "set-membership-calc", 2, dnaEntityType)
        dnaSetBatchCalc = lilyCommand(tenant, "dna-set-batch-calc", 2, dnaEntityType)
        dnaItxBatchCalc >> dnaEntityBatchCalc >> itxViewBatchCalc >> setMembershipCalc >> dnaSetBatchCalc
        tasks.append(dnaItxBatchCalc)

    itxBatchAugmentation >> tasks
