from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import re
from datetime import datetime


def normal_salary(s):
    s = str(s).lower()
    donvi = "USD" if "usd" in s else "VND" if "triệu" in s else None

    if "thỏa thuận" in s:
        return None, None, donvi

    temp = re.search(r"trên\s*([\d,]+)", s)
    if temp:
        return int(temp.group(1).replace(",", "")), None, donvi

    temp = re.search(r"tới\s*([\d,]+)", s)
    if temp:
        return None, int(temp.group(1).replace(",", "")), donvi

    temp = re.search(r"([\d,]+)\s*-\s*([\d,]+)", s)
    if temp:
        return int(temp.group(1).replace(",", "")), int(temp.group(2).replace(",", "")), donvi

    temp = re.search(r"([\d,]+)", s)
    if temp:
        salary = int(temp.group(1).replace(",", ""))
        return salary, salary, donvi

    return None, None, donvi


def process_address(s):
    parts = str(s).split(": ")
    return parts[0], parts[1] if len(parts) > 1 else None


def nor_job_title(title):
    title = str(title).lower().strip()
    mapping = {
        "software developer": ["developer", "software engineer", "programmer", "full-stack developer", "php"],
        "business analyst": ["business analyst", "chuyên viên business analyst", "ba"],
        "data scientist": ["data scientist", "machine learning engineer", "ai engineer"],
        "project manager": ["project manager", "it project manager"],
        ".net developer": [".net developer", "lập trình viên .net"],
        "it support": ["it helpdesk", "it application support"],
        "devops engineer": ["devops", "sre", "chuyên viên quản trị hệ thống"],
    }
    for norm_title, keywords in mapping.items():
        if any(keyword in title for keyword in keywords):
            return norm_title
    return title.strip()


def extract_transform():
    df = pd.read_csv("/home/namngo/airflow/data/data.csv")
    df[['min_salary', 'max_salary', 'salary_unit']] = df['salary'].apply(lambda x: pd.Series(normal_salary(x)))
    df[['city', 'district']] = df['address'].apply(lambda x: pd.Series(process_address(x)))
    df['normalized_job_title'] = df['job_title'].apply(nor_job_title)
    df.to_csv("/home/namngo/airflow/data/cleaned_data.csv", index=False)


def load_to_postgres():
    df = pd.read_csv("/home/namngo/airflow/data/cleaned_data.csv")
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    print("Engine:", engine)
    with engine.connect() as connection:
        df.to_sql("job_data", engine, if_exists="replace", index=False)


with DAG(
        dag_id="etl_pandas_postgres",
        schedule_interval="@daily",
        start_date=datetime(2024, 3, 1),
        catchup=False,
) as dag:
    extract_transform_task = PythonOperator(
        task_id="extract_transform",
        python_callable=extract_transform
    )
    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )
    extract_transform_task >> load_task
