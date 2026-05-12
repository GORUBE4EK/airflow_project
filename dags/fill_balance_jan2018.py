from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from sqlalchemy import text


def fill_balance_for_date(on_date):
    import logging
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    logger = logging.getLogger(__name__)
    postgres_hook = PostgresHook("project")

    try:
        sql = f'CALL "DM".fill_account_balance_f(DATE \'{on_date}\');'
        postgres_hook.run(sql)
        logger.info(f"Витрина остатков заполнена за {on_date}")

    except Exception as e:
        logger.error(f"Ошибка за {on_date}: {str(e)}")
        raise


def init_balance_2017():
    import logging
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    logger = logging.getLogger(__name__)
    postgres_hook = PostgresHook("project")

    init_sql = """
    DELETE FROM "DM".DM_ACCOUNT_BALANCE_F WHERE on_date = '2017-12-31';

    INSERT INTO "DM".DM_ACCOUNT_BALANCE_F (on_date, account_rk, balance_out, balance_out_rub)
    SELECT 
        b.on_date,
        b.account_rk,
        b.balance_out,
        b.balance_out * COALESCE(
            (SELECT er.reduced_cource
             FROM "DS".MD_EXCHANGE_RATE_D AS er
             WHERE er.currency_rk = b.currency_rk
                 AND DATE '2017-12-31' BETWEEN er.data_actual_date AND er.data_actual_end_date),
            1
        ) AS balance_out_rub
    FROM "DS".FT_BALANCE_F AS b
    WHERE b.on_date = '2017-12-31';
    """

    postgres_hook.run(init_sql)
    logger.info("Начальные остатки за 31.12.2017 заполнены")

default_args = {
    'owner': 'AGolubev',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='fill_balance_jan2018',
    default_args=default_args,
    catchup=False,
    schedule=None,
    description='Расчёт витрины остатков за январь 2018'
) as dag:


    init_balance = PythonOperator(
        task_id='init_balance_20171231',
        python_callable=init_balance_2017
    )

    tasks = []
    start_date = datetime(2018, 1, 1)
    end_date = datetime(2018, 1, 31)

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')

        task = PythonOperator(
            task_id=f'balance_{date_str.replace("-", "_")}',
            python_callable=fill_balance_for_date,
            op_kwargs={'on_date': date_str}
        )

        tasks.append(task)
        current_date += timedelta(days=1)

    init_balance >> tasks[0]
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]