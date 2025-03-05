from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import re
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
# Kết nối PostgreSQL
POSTGRES_CONN = "postgresql+psycopg2://postgres:password@localhost:5432/my_db"

# Đường dẫn file CSV
CSV_PATH = "/home/namngo/airflow/data/data.csv"

# Hàm chuẩn hóa lương
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

# Hàm chuẩn hóa địa chỉ
def process_address(s):
    parts = str(s).split(": ")
    return (parts[0], parts[1]) if len(parts) >= 2 else (parts[0], None)

# Chuẩn hóa job title
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

# Task 1: Extract - Đọc dữ liệu từ CSV
def extract_data():
    df = pd.read_csv(CSV_PATH, encoding="utf-8")
    df.to_csv("/home/namngo/airflow/data/raw_data.csv", index=False)  # Lưu tạm thời
    print("Dữ liệu đã được trích xuất!")

# Task 2: Transform - Xử lý dữ liệu
def transform_data():
    df = pd.read_csv("/home/namngo/airflow/data/raw_data.csv")

    df[['min_salary', 'max_salary', 'salary_unit']] = list(map(normal_salary, df['salary']))
    df[['city', 'district']] = list(map(process_address, df['address']))
    df['normalized_job_title'] = df['job_title'].map(nor_job_title)
    df['processed_at'] = datetime.now()

    df.to_csv("/home/namngo/airflow/data/transformed_data.csv", index=False)
    print("Dữ liệu đã được chuẩn hóa!")

# Task 3: Load - Lưu dữ liệu vào PostgreSQL
from sqlalchemy import create_engine


def load_data():
    engine = create_engine(POSTGRES_CONN)

    try:
        df = pd.read_csv("/home/namngo/airflow/data/transformed_data.csv")

        # Sử dụng pandas method
        df.to_sql("job_data", engine, if_exists="replace", index=False)

        print("Dữ liệu đã được tải vào PostgreSQL!")

    except Exception as e:
        print(f"Lỗi khi tải dữ liệu: {e}")
        raise
# Định nghĩa DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    task_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    task_load = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    # Thiết lập thứ tự chạy các task
    task_extract >> task_transform >> task_load

