from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta, timezone
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
import os

default_args = {
    'owner': 'AGolubev',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

def load_csv_to_db():
    import logging

    logger = logging.getLogger(__name__)
    postgreshook = PostgresHook("project")
    engine = postgreshook.get_sqlalchemy_engine()

    msk = timezone(timedelta(hours=3))
    start_dt = datetime.now(msk)
    start_time = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    procedure_name = 'import.dm_f101_round_f_v2'

    postgreshook.run(f"""
        INSERT INTO "LOGS".LOGS (procedure_name, start_time, message)
        VALUES ('{procedure_name}', '{start_time}', 'Начало загрузки CSV в DM_F101_ROUND_F_V2');
    """)

    try:
        file_path = "/opt/airflow/files/dm_f101_round_f.csv"

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл не найден: {file_path}")

        df = pd.read_csv(file_path, delimiter=";", encoding="cp1251")
        logger.info(f"Прочитано строк из CSV: {len(df)}")
        logger.info(f"Колонки: {df.columns.tolist()}")

        before_count = len(df)
        df = df.drop_duplicates()
        after_count = len(df)
        if before_count != after_count:
            logger.warning(f"Удалено дубликатов: {before_count - after_count}")

        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS "DM".dm_f101_round_f_v2 
                (LIKE "DM".dm_f101_round_f INCLUDING ALL);
            """))
            logger.info('Таблица "DM".DM_F101_ROUND_F_V2 проверена/создана')

        with engine.begin() as conn:
            conn.execute(text('DELETE FROM "DM".DM_F101_ROUND_F_V2'))
            logger.info('Таблица "DM".DM_F101_ROUND_F_V2 очищена')

        df.to_sql(
            "dm_f101_round_f_v2",
            engine,
            schema="DM",
            if_exists="append",
            index=False
        )

        end_dt = datetime.now(msk)
        end_time = end_dt.strftime('%Y-%m-%d %H:%M:%S')
        duration = (end_dt - start_dt).total_seconds()

        postgreshook.run(f"""
            INSERT INTO "LOGS".LOGS (procedure_name, start_time, end_time, message)
            VALUES ('{procedure_name}', '{start_time}', '{end_time}', 
                    'Успешно загружено {len(df)} строк в DM_F101_ROUND_F_V2 за {duration:.2f} сек');
        """)

        logger.info(f"Загружено в DM_F101_ROUND_F_V2: {len(df)} строк за {duration:.2f} сек")

    except Exception as e:
        end_dt = datetime.now(msk)
        end_time = end_dt.strftime('%Y-%m-%d %H:%M:%S')
        error_msg = str(e).replace("'", "''")[:500]

        postgreshook.run(f"""
            INSERT INTO "LOGS".LOGS (procedure_name, start_time, end_time, message)
            VALUES ('{procedure_name}', '{start_time}', '{end_time}', 
                    'ОШИБКА: {error_msg}');
        """)

        logger.error(f"Ошибка: {str(e)}")
        raise


with DAG(
        dag_id='load_csv_to_db',
        default_args=default_args,
        catchup=False,
        schedule=None,
        description='Загрузка CSV 101 формы обратно в БД (DM_F101_ROUND_F_V2)'
) as dag:

    start = EmptyOperator(task_id="start")

    load_task = PythonOperator(
        task_id='load_f101_v2',
        python_callable=load_csv_to_db
    )

    end = EmptyOperator(task_id='end')

    start >> load_task >> end