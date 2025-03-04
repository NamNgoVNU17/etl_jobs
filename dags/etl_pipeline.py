from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import re
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String, Float
import logging
logger = logging.getLogger(__name__)



DB_URL = "postgresql://postgres:password@localhost:5432/my_db"

def etl_pipeline():

    try :
        df = pd.read_csv('/home/namngo/project/prjUnigap1/data/data.csv')
    except Exception as e :
        logger.error(f"Error read file CSV : {e}")
        return

    try :
        def normal_salary(s):
            s = str(s).lower()
            donvi = "USD" if "usd" in s else "VND" if "triệu" in s else None
            if "thỏa thuận" in s:
                return None, None, donvi
            temp = re.search(r"trên\s*([\d,]+)", s)
            if temp:
                min_salary = int(temp.group(1).replace(",", ""))
                return min_salary, None, donvi
            temp = re.search(r"tới\s*([\d,]+)", s)
            if temp:
                max_salary = int(temp.group(1).replace(",", ""))
                return None, max_salary, donvi
            temp = re.search(r"([\d,]+)\s*-\s*([\d,]+)", s)
            if temp:
                min_salary = int(temp.group(1).replace(",", ""))
                max_salary = int(temp.group(2).replace(",", ""))
                return min_salary, max_salary, donvi
            temp = re.search(r"([\d,]+)", s)
            if temp:
                salary = int(temp.group(1).replace(",", ""))
                return salary, salary, donvi
            return None, None, None

        df[['min_salary', 'max_salary', 'salary_unit']] = df['salary'].apply(lambda x: pd.Series(normal_salary(x)))


        def process_address(s):
            parts = str(s).split(": ")
            if len(parts) >= 2:
                return parts[0], parts[1]
            if len(parts) == 1:
                return parts[0], None
            return None, None

        df[['city', 'district']] = df['address'].apply(lambda x: pd.Series(process_address(x)))


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

        df['normalized_job_title'] = df['job_title'].apply(nor_job_title)
        pass
    except Exception as e :
        logger.error(f"Error transform data: {e}")
        return

    try :
        engine = create_engine(DB_URL)
        df.to_sql('job_data', engine, if_exists='replace', index=False,
                  dtype={"min_salary": Integer, "max_salary": Integer, "salary_unit": String})
    except Exception as e:
        logger.error(f"Error to load data : {e}")
        return

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline cho dữ liệu tuyển dụng',
    schedule_interval='0 2 * * *',  # Chạy lúc 2 giờ sáng hàng ngày
    catchup=False
)

etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=etl_pipeline,
    dag=dag,
)

etl_task
