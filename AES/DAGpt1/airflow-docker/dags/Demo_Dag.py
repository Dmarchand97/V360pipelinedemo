from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


def _print_line():
    print('Hello World')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'catchup': False,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'dag_demo',
    default_args=default_args,
    schedule_interval='* * * * *'
)

# use the print line function in the DAG
print_line = PythonOperator(
    task_id='print_line',
    python_callable=_print_line,
    dag=dag
)


spark_config = {
    'conf': {
        "spark.yarn.maxAppAttempts": "1",
        "spark.yarn.executor.memoryOverhead": "512"
    },
    "conn_id": "spark_local",
    "application": "/Users/dan/PycharmProjects/AES/DAGpt1/airflow-docker/dags/demo_pyspark.py",
    "driver_memory": "1g",
    "executor_cores": 1,
    "num_executors": 1,
    "executor_memory": "1g"
}

spark_operator = SparkSubmitOperator(task_id='spark_submit_task', dag=dag, **spark_config)

print_line.set_downstream(spark_operator)

if __name__ == "__main__":
    dag.cli()
