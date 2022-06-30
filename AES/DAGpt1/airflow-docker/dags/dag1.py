from random import randint
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import datetime as dt


def _best_model(ti):
    accuracies = ti.xcom_pull(
        task_ids=['model_A', 'model_B', 'model_C']
    )
    best_accuracy = max(accuracies)
    if best_accuracy > 8:
        return 'accurate'
    else:
        return 'inaccurate'


def _training_model():
    return randint(1, 10)


with DAG('dag1', start_date=dt.datetime(2022, 1, 1), schedule_interval="@daily") as dag:
    model_A = PythonOperator(
        task_id='model_A',
        python_callable=_training_model
    )

    model_B = PythonOperator(
        task_id='model_B',
        python_callable=_training_model
    )

    model_C = PythonOperator(
        task_id='model_C',
        python_callable=_training_model
    )

    best_model = BranchPythonOperator(
        task_id='best_model',
        python_callable=_best_model
    )

    accurate = BashOperator(
        task_id='accurate',
        bash_command="echo 'accurate'"
    )

    inaccurate = BashOperator(
        task_id='inaccurate',
        bash_command="echo 'inaccurate'"
    )

    # >> downstream
    # << upstream

    [model_A, model_B, model_C] >> best_model >> [accurate, inaccurate]