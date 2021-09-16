

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

from airflow.utils.dates import days_ago


with DAG(
    "DAG_Bash_Op", schedule_interval='@once', catchup=False,  start_date=days_ago(1)
) as dag:


    execute_command=BashOperator(
    task_id='execute_command',
    bash_command="scripts/commands.sh"

    # uses scripts held in scripts/commands.sh file
    )


  
