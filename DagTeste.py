from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="minha_primeira_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["teste"],
) as dag:

    tarefa = BashOperator(task_id="dizer_ola", bash_command="echo 'Olá do Airflow'",)

