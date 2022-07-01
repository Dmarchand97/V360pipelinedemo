from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


def _print_line():
    print('Hello World')


# just a visual
def _spark_script_here():
    print('Where spark operator would go in graph')


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
spark_script_here = PythonOperator(
    task_id='spark_script_here',
    python_callable=_spark_script_here,
    dag=dag
)

print_line >> spark_script_here

# spark configuration starts

# the configuration for the spark file
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
# The commented lines below would connect the pyspark script to the DAG and would run once a minute
# This is what would replace the _spark_script_here function above
# spark_operator = SparkSubmitOperator(task_id='spark_submit_task', dag=dag, **spark_config)
#
# print_line.set_downstream(spark_operator)

if __name__ == "__main__":
    dag.cli()
