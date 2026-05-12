from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from sqlalchemy import text
from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert

def insert_data(table_name):
    import logging
    import os
    import sys
    import traceback
    import pandas as pd
    from datetime import datetime as dt, timedelta, timezone

    logger = logging.getLogger(__name__)

    sys.stderr.write(f"========== ФУНКЦИЯ ВЫЗВАНА ДЛЯ: {table_name} ==========\n")
    sys.stderr.flush()

    postgreshook = PostgresHook("project")
    engine = postgreshook.get_sqlalchemy_engine()

    msk = timezone(timedelta(hours=3))
    start_dt = dt.now(msk)
    start_time = dt.now(msk).strftime('%Y-%m-%d %H:%M:%S')

    pk_map = {
        'ft_balance_f': ['on_date', 'account_rk'],
        'ft_posting_f': None,
        'md_account_d': ['data_actual_date', 'account_rk'],
        'md_currency_d': ['data_actual_date', 'currency_rk'],
        'md_exchange_rate_d': ['data_actual_date', 'currency_rk'],
        'md_ledger_account_s': ['ledger_account', 'start_date']
    }

    postgreshook.run(f"""
        INSERT INTO "LOGS".LOGS (procedure_name, start_time, message)
        VALUES ('insert_data.{table_name}', '{start_time}', 'Начало загрузки таблицы {table_name}');
    """)

    try:
        file_path = f"/opt/airflow/files/{table_name}.csv"
        logger.info(f"Начинаю загрузку файла: {file_path}")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл не найден: {file_path}")

        encodings_to_try = ['utf-8', 'cp1251','iso-8859-1']
        df = None
        for encoding in encodings_to_try:
            try:
                df = pd.read_csv(file_path, delimiter=";", encoding=encoding, dtype=str)
                logger.info(f"Файл {table_name} прочитан с кодировкой: {encoding}")
                break
            except (UnicodeDecodeError, Exception):
                continue

        if df is None:
            raise ValueError(f"Не удалось прочитать {table_name}")

        df.columns = [col.lower() for col in df.columns]

        logger.info(f"Колонки {table_name}: {df.columns.tolist()}")
        logger.info(f"Загружено строк: {len(df)}")

        if df.empty:
            logger.warning(f"Файл {table_name} пустой — пропускаю")
            end_dt = dt.now(msk)
            end_time = end_dt.strftime('%Y-%m-%d %H:%M:%S')
            postgreshook.run(f"""
                            INSERT INTO "LOGS".LOGS (procedure_name, start_time, end_time, message)
                            VALUES ('insert_data.{table_name}', '{start_time}', '{end_time}', 'Файл пуст, пропущено');
                        """)
            return

        before_count = len(df)
        df = df.drop_duplicates()
        after_count = len(df)
        if before_count != after_count:
            logger.warning(f"Удалено дубликатов в {table_name}: {before_count - after_count}")

        pk = pk_map.get(table_name)

        if pk is None:
            with engine.begin() as conn:
                conn.execute(text(f'DELETE FROM "DS".{table_name}'))
            df.to_sql(table_name, engine, schema="DS", if_exists="append", index=False)
            logger.info(f"Таблица {table_name} очищена и заполнена ({len(df)} строк)")
        else:
            metadata = MetaData(schema="DS")
            table = Table(table_name, metadata, autoload_with=engine)

            with engine.begin() as conn:
                for _, row in df.iterrows():
                    stmt = pg_insert(table).values(row.to_dict())
                    update_cols = {col: stmt.excluded[col] for col in df.columns if col not in pk}
                    stmt = stmt.on_conflict_do_update(
                        index_elements=pk,
                        set_=update_cols
                    )
                    conn.execute(stmt)

            logger.info(f"Таблица {table_name}: выполнен UPSERT ({len(df)} строк)")

        end_dt = dt.now(msk)
        end_time = end_dt.strftime('%Y-%m-%d %H:%M:%S')
        duration = (end_dt - start_dt).total_seconds()

        postgreshook.run(f"""
                   INSERT INTO "LOGS".LOGS (procedure_name, start_time, end_time, message)
                   VALUES ('insert_data.{table_name}', '{start_time}', '{end_time}', 
                           'Успешно загружено {len(df)} строк за {duration:.2f} сек (режим: {"UPSERT" if pk else "DELETE+INSERT"})');
               """)

        logger.info(f"{table_name}: {len(df)} строк за {duration:.2f} сек")

    except Exception as e:
        end_dt = dt.now(msk)
        end_time = end_dt.strftime('%Y-%m-%d %H:%M:%S')
        error_msg = str(e).replace("'", "''")[:500]

        postgreshook.run(f"""
                   INSERT INTO "LOGS".LOGS (procedure_name, start_time, end_time, message)
                   VALUES ('insert_data.{table_name}', '{start_time}', '{end_time}', 
                           'ОШИБКА: {error_msg}');
               """)

        logger.error(f"ОШИБКА в {table_name}: {str(e)}")
        logger.error(traceback.format_exc())
        raise


default_args = {
    'owner': 'AGolubev',
    'start_date': datetime(2024, 1, 1),
    'retries': 2
}

with DAG(
        dag_id='insert_data',
        default_args=default_args,
        catchup=False,
        schedule=None,
) as dag:
    start = EmptyOperator(task_id="start")

    ft_balance_f = PythonOperator(
        task_id="ft_balance_f",
        python_callable=insert_data,
        op_kwargs={"table_name": "ft_balance_f"},
    )

    ft_posting_f = PythonOperator(
        task_id="ft_posting_f",
        python_callable=insert_data,
        op_kwargs={"table_name": "ft_posting_f"},
    )

    md_account_d = PythonOperator(
        task_id="md_account_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_account_d"},
    )

    md_currency_d = PythonOperator(
        task_id="md_currency_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_currency_d"},
    )

    md_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_exchange_rate_d"},
    )

    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_ledger_account_s"},
    )

    end = EmptyOperator(task_id='end')

    start >> [
        ft_balance_f,
        ft_posting_f,
        md_account_d,
        md_currency_d,
        md_exchange_rate_d,
        md_ledger_account_s
    ] >> end