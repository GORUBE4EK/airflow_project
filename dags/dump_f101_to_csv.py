from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os

default_args = {
    'owner': 'AGolubev',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}


def dump_to_csv():
    import logging
    from datetime import datetime as dt, timedelta, timezone

    logger = logging.getLogger(__name__)
    postgreshook = PostgresHook("project")

    msk = timezone(timedelta(hours=3))
    start_dt = datetime.now(msk)
    start_time = dt.now(msk).strftime('%Y-%m-%d %H:%M:%S')
    procedure_name = 'export.dm_f101_round_f'

    postgreshook.run(f"""
        INSERT INTO "LOGS".LOGS (procedure_name, start_time, message)
        VALUES ('{procedure_name}', '{start_time}', 'Начало выгрузки DM_F101_ROUND_F в CSV');
    """)

    try:
        engine = postgreshook.get_sqlalchemy_engine()
        df = pd.read_sql('SELECT * FROM "DM".DM_F101_ROUND_F ORDER BY ledger_account', engine)

        logger.info(f"Загружено строк из БД: {len(df)}")

        output_path = "/opt/airflow/files/dm_f101_round_f.csv"
        df.to_csv(output_path, sep=";", index=False, encoding="cp1251")

        file_size = os.path.getsize(output_path)
        end_dt = datetime.now(msk)
        end_time = dt.now(msk).strftime('%Y-%m-%d %H:%M:%S')
        duration = (end_dt - start_dt).total_seconds()

        postgreshook.run(f"""
            INSERT INTO "LOGS".LOGS (procedure_name, start_time, end_time, message)
            VALUES ('{procedure_name}', '{start_time}', '{end_time}', 
                    'Выгружено {len(df)} строк, размер файла: {file_size} байт, время: {duration:.2f} сек');
        """)

        logger.info(f"Сохранено в {output_path} | {len(df)} строк | {file_size} байт | {duration:.2f} сек")

    except Exception as e:
        end_time = dt.now(msk).strftime('%Y-%m-%d %H:%M:%S')
        error_msg = str(e).replace("'", "''")[:500]

        postgreshook.run(f"""
            INSERT INTO "LOGS".LOGS (procedure_name, start_time, end_time, message)
            VALUES ('{procedure_name}', '{start_time}', '{end_time}', 
                    'ОШИБКА: {error_msg}');
        """)

        logger.error(f"Ошибка: {str(e)}")
        raise


with DAG(
        dag_id='dump_f101_to_csv',
        default_args=default_args,
        catchup=False,
        schedule=None,
        description='Выгрузка DM_F101_ROUND_F в CSV'
) as dag:

    start = EmptyOperator(task_id="start")

    dump_task = PythonOperator(
        task_id='dump_f101',
        python_callable=dump_to_csv
    )

    end = EmptyOperator(task_id='end')

start >> dump_task >> end