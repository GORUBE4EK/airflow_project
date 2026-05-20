from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

FILES_PATH = '/opt/airflow/files'

# Все таблицы для загрузки
TABLE_NAMES = [
    'ft_balance_f',
    'ft_posting_f',
    'md_account_d',
    'md_currency_d',
    'md_exchange_rate_d',
    'md_ledger_account_s'
]


def load_csv_to_table(table_name, **context):
    """Загрузка CSV файла в таблицу через COPY"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    file_path = os.path.join(FILES_PATH, f'{table_name}.csv')

    # Определяем маппинг для каждой таблицы
    if table_name == 'ft_balance_f':
        cursor.execute("""
            COPY dsl.ft_balance_f(account_rk, currency_rk, balance_out, on_date)
            FROM STDIN WITH CSV HEADER DELIMITER ','
        """)
    elif table_name == 'ft_posting_f':
        cursor.execute("""
            COPY dsl.ft_posting_f(credit_account_rk, debet_account_rk, 
                                  credit_amount, debet_amount, oper_date)
            FROM STDIN WITH CSV HEADER DELIMITER ','
        """)
    elif table_name == 'md_account_d':
        cursor.execute("""
            COPY dsl.md_account_d(account_rk, account_number, char_type, 
                                 currency_rk, data_actual_date, data_actual_end_date)
            FROM STDIN WITH CSV HEADER DELIMITER ','
        """)
    elif table_name == 'md_currency_d':
        cursor.execute("""
            COPY dsl.md_currency_d(currency_rk, currency_code, code_name, 
                                  currency_name, data_actual_date, data_actual_end_date)
            FROM STDIN WITH CSV HEADER DELIMITER ','
        """)
    elif table_name == 'md_exchange_rate_d':
        cursor.execute("""
            COPY dsl.md_exchange_rate_d(currency_rk, exchange_rate, 
                                       data_actual_date, data_actual_end_date)
            FROM STDIN WITH CSV HEADER DELIMITER ','
        """)
    elif table_name == 'md_ledger_account_s':
        cursor.execute("""
            COPY dsl.md_ledger_account_s(chapter, chapter_name, section_number,
                                        section_name, subsection_name, ledger_account,
                                        ledger_account_name, characteristic,
                                        start_date, end_date, data_actual_date,
                                        data_actual_end_date)
            FROM STDIN WITH CSV HEADER DELIMITER ','
        """)

    with open(file_path, 'r') as f:
        cursor.copy_expert(cursor.query.decode(), f)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Загружено {table_name} из {file_path}")


with DAG(
        'load_all_tables_with_top10_query',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
        description='Загрузка всех таблиц DSL + создание DM + топ-10 проводок за 15.01.2018',
) as dag:
    # Создаем схему DSL
    create_dsl_schema = PostgresOperator(
        task_id='create_dsl_schema',
        postgres_conn_id='postgres_default',
        sql="CREATE SCHEMA IF NOT EXISTS dsl;"
    )

    # Создание таблицы ft_balance_f
    create_balance_table = PostgresOperator(
        task_id='create_balance_table',
        postgres_conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS dsl.ft_balance_f;
            CREATE TABLE dsl.ft_balance_f(
                  balance_id    SERIAL8
                , account_rk    INT8
                , currency_rk   INT8
                , balance_out   NUMERIC(19,2)
                , on_date       DATE
            );
        """
    )

    # Создание таблицы ft_posting_f
    create_posting_table = PostgresOperator(
        task_id='create_posting_table',
        postgres_conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS dsl.ft_posting_f;
            CREATE TABLE dsl.ft_posting_f(
                  posting_id        SERIAL8
                , credit_account_rk INT8
                , debet_account_rk  INT8
                , credit_amount     NUMERIC(19,2)
                , debet_amount      NUMERIC(19,2)
                , oper_date         DATE
            );
        """
    )

    # Создание таблицы md_account_d
    create_account_table = PostgresOperator(
        task_id='create_account_table',
        postgres_conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS dsl.md_account_d;
            CREATE TABLE dsl.md_account_d(
                  account_id            SERIAL8
                , account_rk            INT8
                , account_number        VARCHAR(20)
                , char_type             VARCHAR(1)
                , currency_rk           INT8
                , data_actual_date      DATE
                , data_actual_end_date  DATE
            );
        """
    )

    # Создание таблицы md_currency_d
    create_currency_table = PostgresOperator(
        task_id='create_currency_table',
        postgres_conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS dsl.md_currency_d;
            CREATE TABLE dsl.md_currency_d(
                  currency_id           SERIAL8
                , currency_rk           INT8
                , currency_code         VARCHAR(3)
                , code_name             VARCHAR(10)
                , currency_name         VARCHAR(50)
                , data_actual_date      DATE
                , data_actual_end_date  DATE
            );
        """
    )

    # Создание таблицы md_exchange_rate_d
    create_exchange_table = PostgresOperator(
        task_id='create_exchange_table',
        postgres_conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS dsl.md_exchange_rate_d;
            CREATE TABLE dsl.md_exchange_rate_d(
                  exchange_rate_id      SERIAL8
                , currency_rk           INT8
                , exchange_rate         NUMERIC(19,4)
                , data_actual_date      DATE
                , data_actual_end_date  DATE
            );
        """
    )

    # Создание таблицы md_ledger_account_s
    create_ledger_table = PostgresOperator(
        task_id='create_ledger_table',
        postgres_conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS dsl.md_ledger_account_s;
            CREATE TABLE dsl.md_ledger_account_s(
                  ledger_account_id     SERIAL8
                , chapter               VARCHAR(1)
                , chapter_name          VARCHAR(50)
                , section_number        VARCHAR(5)
                , section_name          VARCHAR(50)
                , subsection_name       VARCHAR(50)
                , ledger_account        VARCHAR(20)
                , ledger_account_name   VARCHAR(100)
                , characteristic        VARCHAR(1)
                , start_date            DATE
                , end_date              DATE
                , data_actual_date      DATE
                , data_actual_end_date  DATE
            );
        """
    )

    # Загрузка данных
    load_balance_data = PythonOperator(
        task_id='load_balance_data',
        python_callable=load_csv_to_table,
        op_kwargs={'table_name': 'ft_balance_f'},
    )

    load_posting_data = PythonOperator(
        task_id='load_posting_data',
        python_callable=load_csv_to_table,
        op_kwargs={'table_name': 'ft_posting_f'},
    )

    load_account_data = PythonOperator(
        task_id='load_account_data',
        python_callable=load_csv_to_table,
        op_kwargs={'table_name': 'md_account_d'},
    )

    load_currency_data = PythonOperator(
        task_id='load_currency_data',
        python_callable=load_csv_to_table,
        op_kwargs={'table_name': 'md_currency_d'},
    )

    load_exchange_data = PythonOperator(
        task_id='load_exchange_data',
        python_callable=load_csv_to_table,
        op_kwargs={'table_name': 'md_exchange_rate_d'},
    )

    load_ledger_data = PythonOperator(
        task_id='load_ledger_data',
        python_callable=load_csv_to_table,
        op_kwargs={'table_name': 'md_ledger_account_s'},
    )

    # Создание DM слоя
    create_dm_schema = PostgresOperator(
        task_id='create_dm_schema',
        postgres_conn_id='postgres_default',
        sql="CREATE SCHEMA IF NOT EXISTS dm;"
    )

    create_dm_table = PostgresOperator(
        task_id='create_dm_table',
        postgres_conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS dm.posting_data_by_date;
            CREATE TABLE dm.posting_data_by_date(
                  operation_date    DATE
                , debit_amount      NUMERIC(19,2)
                , credit_amount     NUMERIC(19,2)
                , row_timestamp     TIMESTAMP
            );
        """
    )

    # Топ-10 проводок за 15.01.2018
    select_top10_postings = PostgresOperator(
        task_id='select_top10_postings',
        postgres_conn_id='postgres_default',
        sql="""
            SELECT 
                fpf.debet_account_rk AS "Номер дебетового счета",
                fpf.credit_account_rk AS "Номер кредитового счета",
                fpf.credit_amount AS "Сумма дебета",
                fpf.debet_amount AS "Сумма кредита"
            FROM dsl.ft_posting_f fpf
            WHERE fpf.oper_date = '2018-01-15'
            ORDER BY fpf.credit_amount DESC, fpf.debet_amount DESC
            LIMIT 10;
        """,
        # Вывод результатов в лог
        log_sql_parameters=False
    )

    # Определяем последовательность
    create_dsl_schema >> [
        create_balance_table,
        create_posting_table,
        create_account_table,
        create_currency_table,
        create_exchange_table,
        create_ledger_table
    ]

    create_balance_table >> load_balance_data
    create_posting_table >> load_posting_data
    create_account_table >> load_account_data
    create_currency_table >> load_currency_data
    create_exchange_table >> load_exchange_data
    create_ledger_table >> load_ledger_data

    # Ждем загрузки всех таблиц перед созданием DM
    [
        load_balance_data,
        load_posting_data,
        load_account_data,
        load_currency_data,
        load_exchange_data,
        load_ledger_data
    ] >> create_dm_schema >> create_dm_table >> select_top10_postings