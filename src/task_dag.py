from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 8, 14),
    'retry_delay': timedelta(seconds=60 * 60)
}

with DAG('task_dag', catchup=False, default_args=default_args, schedule_interval="@daily") as dag:

    task1 = DummyOperator(task_id="task1", dag=dag)
    task2 = DummyOperator(task_id="task2", dag=dag)
    task3 = DummyOperator(task_id="task3", dag=dag)
    task4 = DummyOperator(task_id="task4", dag=dag)
    task5 = DummyOperator(task_id="task5", dag=dag)
    task6 = DummyOperator(task_id="task6", dag=dag)

task1 >> {task2,task3} >> {task4,task5,task6}
