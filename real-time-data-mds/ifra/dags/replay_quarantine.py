from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "replay_quarantine",
    default_args=default_args,
    description="Replay quarantined events back to Kafka or S3.",
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dlq", "replay", "quarantine"],
) as dag:
    replay = BashOperator(
        task_id="replay_quarantine",
        bash_command=(
            "python /opt/airflow/dags/../scripts/replay_quarantine.py"
        ),
        env={
            "ENV_FILE": "/opt/airflow/.env",
            "REPLAY_MODE": "kafka",
            "REPLAY_TOPIC": "stock-quotes",
            "REPLAY_HOURS": "24",
        },
    )
