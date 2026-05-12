from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from sqlalchemy import text


def fill_f101_for_date(on_date):
    import logging
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    logger = logging.getLogger(__name__)
    postgres_hook = PostgresHook("project")

    try:
        sql = f'CALL "DM".fill_f101_round_f(DATE \'{on_date}\');'
        postgres_hook.run(sql)
        logger.info(f"Форма 101 заполнена за январь")

    except Exception as e:
        logger.error(f"Ошибка при заполнении: {str(e)}")
        raise

default_args = {
    'owner': 'AGolubev',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='fill_f101_jan2018',
    default_args=default_args,
    catchup=False,
    schedule=None,
    description='Расчёт 101 формы за январь 2018'
) as dag:

    start = EmptyOperator(task_id="start")

    fill_f101 = PythonOperator(
        task_id='fill_f101_january_2018',
        python_callable=fill_f101_for_date,
        op_kwargs={'on_date': '2018-02-01'}
    )

    end = EmptyOperator(task_id='end')

start >> fill_f101  >> end
