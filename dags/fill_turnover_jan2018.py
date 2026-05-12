from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from sqlalchemy import text


def fill_turnover_for_date(on_date):
    import logging
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    logger = logging.getLogger(__name__)
    postgres_hook = PostgresHook("project")

    try:
        sql = f'SELECT "DM".fill_account_turnover_f(DATE \'{on_date}\');'
        postgres_hook.run(sql)
        logger.info(f"Витрина оборотов заполнена за {on_date}")

    except Exception as e:
        logger.error(f"Ошибка за {on_date}: {str(e)}")
        raise

default_args = {
    'owner': 'AGolubev',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='fill_turnover_jan2018',
    default_args=default_args,
    catchup=False,
    schedule=None,
    description='Расчёт витрины оборотов за январь 2018'
) as dag:

    tasks = []
    start_date = datetime(2018, 1, 1)
    end_date = datetime(2018, 1, 31)

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')

        task = PythonOperator(
            task_id=f'turnover_{date_str.replace("-", "_")}',
            python_callable=fill_turnover_for_date,
            op_kwargs={'on_date': date_str}
        )

        tasks.append(task)
        current_date += timedelta(days=1)

    tasks[0]
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]