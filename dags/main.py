from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    'owner': 'AGolubev',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
        dag_id='master_pipeline',
        default_args=default_args,
        catchup=False,
        schedule=None,
        description='Главный пайплайн для запуска всех DAGов'
) as dag:

    start = EmptyOperator(task_id="start")

    load_data = TriggerDagRunOperator(
        task_id='load_data',
        trigger_dag_id='insert_data',
        wait_for_completion=True,
        reset_dag_run=True
    )


    calc_turnover = TriggerDagRunOperator(
        task_id='calc_turnover',
        trigger_dag_id='fill_turnover_jan2018',
        wait_for_completion=True,
        reset_dag_run=True
    )


    calc_balance = TriggerDagRunOperator(
        task_id='calc_balance',
        trigger_dag_id='fill_balance_jan2018',
        wait_for_completion=True,
        reset_dag_run=True
    )


    calc_f101 = TriggerDagRunOperator(
        task_id='calc_f101',
        trigger_dag_id='fill_f101_jan2018',
        wait_for_completion=True,
        reset_dag_run=True
    )


    export_csv = TriggerDagRunOperator(
        task_id='export_csv',
        trigger_dag_id='dump_f101_to_csv',
        wait_for_completion=True,
        reset_dag_run=True
    )

    end = EmptyOperator(task_id='end')

    start >> load_data >> calc_turnover >> calc_balance >> calc_f101 >> export_csv >> end