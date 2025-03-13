from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import pandas as pd
import re
from datetime import datetime
import os
import logging
from tenacity import retry, stop_after_attempt, wait_fixed
import json

def load_config():
    config_path = Variable.get("config_path", "/home/namngo/airflow/config.json")
    with open(config_path, "r") as f:
        return json.load(f)

config = load_config()

def get_file_path(file_name):
    return os.path.join(config.get("data_path", "/home/namngo/airflow/data/"), file_name)

def normal_salary(s):
    try:
        s = str(s).lower()
        donvi = "USD" if "usd" in s else "VND" if "triệu" in s else None
        if "thỏa thuận" in s:
            return None, None, donvi
        temp = re.search(r"([\d,]+)\s*(?:triệu|usd)?\s*-\s*([\d,]+)\s*(?:triệu|usd)?", s)
        if temp:
            return int(temp.group(1).replace(",", "")), int(temp.group(2).replace(",", "")), donvi
        temp = re.search(r"trên\s*([\d,]+)", s)
        if temp:
            return int(temp.group(1).replace(",", "")), None, donvi
        temp = re.search(r"tới\s*([\d,]+)", s)
        if temp:
            return None, int(temp.group(1).replace(",", "")), donvi
        temp = re.search(r"([\d,]+)", s)
        if temp:
            return int(temp.group(1).replace(",", "")), int(temp.group(1).replace(",", "")), donvi
    except Exception as e:
        logging.error(f"Lỗi xử lý lương: {e}")
    return None, None, donvi

def process_address(s):
    try:
        parts = str(s).split(": ")
        return (parts[0], parts[1]) if len(parts) >= 2 else (parts[0], None)
    except Exception as e:
        logging.error(f"Lỗi xử lý địa chỉ: {e}")
        return None, None

def extract_transform():
    try:
        file_path = get_file_path("data.csv")
        if not os.path.exists(file_path):
            logging.error(f"File không tồn tại: {file_path}")
            return
        chunk_size = 10000
        processed_file = get_file_path("processed_data.csv")
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            chunk[['min_salary', 'max_salary', 'salary_unit']] = chunk['salary'].apply(lambda x: pd.Series(normal_salary(x)))
            chunk[['city', 'district']] = chunk['address'].apply(lambda x: pd.Series(process_address(x)))
            chunk['processed_at'] = datetime.now()
            chunk.to_csv(processed_file, mode='a', index=False, header=not os.path.exists(processed_file))
        logging.info(f"Dữ liệu đã được xử lý và lưu vào {processed_file}")
    except Exception as e:
        logging.error(f"Lỗi trong quá trình Extract-Transform: {e}")

@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
def load_to_postgres():
    try:
        file_path = get_file_path("processed_data.csv")
        if not os.path.exists(file_path):
            logging.error(f"File không tồn tại: {file_path}")
            return
        chunk_size = 5000
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS job_data (
            id SERIAL PRIMARY KEY,
            salary TEXT,
            min_salary FLOAT,
            max_salary FLOAT,
            salary_unit TEXT,
            address TEXT,
            city TEXT,
            district TEXT,
            job_title TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_query)
        conn.commit()

        insert_query = """
        INSERT INTO job_data (salary, min_salary, max_salary, salary_unit, address, city, district, job_title, processed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            chunk = chunk.where(pd.notna(chunk), None)
            chunk = chunk[['salary', 'min_salary', 'max_salary', 'salary_unit',
                              'address', 'city', 'district', 'job_title', 'processed_at']]
            data_tuples = [tuple(x) for x in chunk.to_numpy()]
            cursor.executemany(insert_query, data_tuples)
            conn.commit()
        logging.info(f"Đã load dữ liệu vào PostgreSQL.")
    except Exception as e:
        logging.error(f"Lỗi khi tải dữ liệu vào PostgreSQL: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

default_args = {"owner": "airflow", "start_date": datetime(2024, 3, 13), "catchup": False, "retries": 3, "retry_delay": 10}

dag = DAG(
    "etl_to_postgres",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

extract_transform_task = PythonOperator(
    task_id="extract_transform",
    python_callable=extract_transform,
    dag=dag
)

load_task = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_to_postgres,
    dag=dag
)

extract_transform_task >> load_task
