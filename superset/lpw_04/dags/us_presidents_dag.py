"""
DAG для анализа президентов США
Вариант задания №30

Автор: Студент
Дата: 2024
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import math

# Конфигурация по умолчанию для DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Создание DAG
dag = DAG(
    'us_presidents_analysis',
    default_args=default_args,
    description='Анализ президентов США - вариант 30',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'us_presidents', 'kaggle', 'variant_30']
)


def extract_from_kaggle(**context):
    """
    Extract: Скачивание данных о президентах США с Kaggle
    """
    import os
    import shutil
    import kagglehub
    
    print("Начинаем извлечение данных о президентах США с Kaggle...")
    
    DATA_DIR = '/opt/airflow/dags/data'
    os.makedirs(DATA_DIR, exist_ok=True)
    
    kaggle_json_path = '/home/airflow/.kaggle/kaggle.json'
    if not os.path.exists(kaggle_json_path):
        raise FileNotFoundError(f"Файл kaggle.json не найден в {kaggle_json_path}.")
    print(f"kaggle.json найден в: {kaggle_json_path}")
    
    dataset_name = "harshitagpt/us-presidents"
    print(f"Скачиваем датасет: {dataset_name}")
    
    path = kagglehub.dataset_download(dataset_name)
    print(f"Данные скачааны в: {path}")
    
    source_file = os.path.join(path, "us_presidents.csv")
    dest_file = os.path.join(DATA_DIR, "us_presidents.csv")
    shutil.copy2(source_file, dest_file)
    print(f"Файл скопирован в: {dest_file}")
    
    context['task_instance'].xcom_push(key='data_file_path', value=dest_file)
    print("Данные о президентах успешно извлечены с Kaggle")
    return dest_file

def load_raw_to_postgres(**context):
    """
    Load Raw: Загрузка сырых данных в PostgreSQL
    """
    import pandas as pd
    
    print("Начинаем загрузку сырых данных в PostgreSQL...")
    data_file_path = context['task_instance'].xcom_pull(key='data_file_path', task_ids='extract_from_kaggle')
    df = pd.read_csv(data_file_path)
    print(f"Загружено {len(df)} записей из файла")

    postgres_hook = PostgresHook(postgres_conn_id='analytics_postgres')
    
    column_mapping = {
        'S.No.': 's_no', 'start': 'start_date', 'end': 'end_date',
        'president': 'president', 'prior': 'prior', 'party': 'party', 'vice': 'vice'
    }
    df.rename(columns=column_mapping, inplace=True)

    allowed_columns = ['s_no', 'start_date', 'end_date', 'president', 'prior', 'party', 'vice']
    df_clean = df[[col for col in allowed_columns if col in df.columns]]
    
    postgres_hook.run("DROP TABLE IF EXISTS raw_us_presidents;")
    create_table_sql = """
    CREATE TABLE raw_us_presidents (
        id SERIAL PRIMARY KEY, s_no INTEGER, start_date TEXT, end_date TEXT,
        president TEXT, prior TEXT, party TEXT, vice TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    postgres_hook.run(create_table_sql)
    
    postgres_hook.insert_rows(
        table='raw_us_presidents',
        rows=df_clean.values.tolist(),
        target_fields=list(df_clean.columns)
    )
    print(f"Успешно загружено {len(df_clean)} записей в raw_us_presidents")

def transform_and_clean_data(**context):
    """
    Transform: Очистка данных, преобразование дат и обогащение для дашборда
    """
    import pandas as pd
    import math # Убедимся, что math импортирован
    
    print("Начинаем очистку, трансформацию и обогащение данных...")
    postgres_hook = PostgresHook(postgres_conn_id='analytics_postgres')
    
    df = postgres_hook.get_pandas_df("SELECT * FROM raw_us_presidents;")
    
    df['start_date_clean'] = pd.to_datetime(df['start_date'], errors='coerce')
    df['end_date_clean'] = pd.to_datetime(df['end_date'], errors='coerce')
    
    df.dropna(subset=['start_date_clean', 'end_date_clean'], inplace=True)
    
    # --- ОБОГАЩЕНИЕ ДАННЫХ ---
    df['start_year'] = df['start_date_clean'].dt.year
    df['end_year'] = df['end_date_clean'].dt.year
    df['years_in_office'] = ((df['end_date_clean'] - df['start_date_clean']).dt.days / 365.25).apply(math.floor)
    df['presidency_decade'] = ((df['start_year'] // 10) * 10).astype(str) + 's'
    
    clean_columns = ['president', 'party', 'start_year', 'end_year', 'years_in_office', 'presidency_decade']
    df_clean = df[clean_columns]
    
    # --- ИСПРАВЛЕНИЕ ЗДЕСЬ ---
    # Удаляем таблицу и все зависимые объекты (нашу VIEW)
    postgres_hook.run("DROP TABLE IF EXISTS stg_us_presidents CASCADE;")
    
    create_staging_table_sql = """
    CREATE TABLE stg_us_presidents (
        president TEXT,
        party TEXT,
        start_year INTEGER,
        end_year INTEGER,
        years_in_office INTEGER,
        presidency_decade TEXT
    );
    """
    postgres_hook.run(create_staging_table_sql)
    
    postgres_hook.insert_rows(
        table='stg_us_presidents',
        rows=df_clean.values.tolist(),
        target_fields=list(df_clean.columns)
    )
    print(f"Успешно загружено {len(df_clean)} обогащенных записей в stg_us_presidents")

# --- Задачи DAG ---

extract_task = PythonOperator(
    task_id='extract_from_kaggle',
    python_callable=extract_from_kaggle,
    dag=dag
)

load_raw_task = PythonOperator(
    task_id='load_raw_to_postgres',
    python_callable=load_raw_to_postgres,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_and_clean_data',
    python_callable=transform_and_clean_data,
    dag=dag
)

create_datamart_task = PostgresOperator(
    task_id='create_datamart',
    postgres_conn_id='analytics_postgres',
    sql='datamart_variant_30.sql',
    dag=dag
)

# --- Определение зависимостей ---
extract_task >> load_raw_task >> transform_task >> create_datamart_task