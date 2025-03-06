import pytest
import pandas as pd
from etl import normal_salary, process_address, nor_job_title, etl_process


def test_normal_salary():
    assert normal_salary("10 triệu - 20 triệu") == (10, 20, "VND")
    assert normal_salary("trên 15 triệu") == (15, None, "VND")
    assert normal_salary("tới 30 triệu") == (None, 30, "VND")
    assert normal_salary("20 triệu") == (20, 20, "VND")
    assert normal_salary("5,000 - 10,000 USD") == (5000, 10000, "USD")
    assert normal_salary("Thỏa thuận") == (None, None, None)


def test_process_address():
    assert process_address("Hà Nội: Cầu Giấy") == ("Hà Nội", "Cầu Giấy")
    assert process_address("Hồ Chí Minh") == ("Hồ Chí Minh", None)


def test_nor_job_title():
    assert nor_job_title("Software Engineer") == "software developer"
    assert nor_job_title("Business Analyst") == "business analyst"
    assert nor_job_title("Machine Learning Engineer") == "data scientist"
    assert nor_job_title("IT Helpdesk") == "it support"
    assert nor_job_title("Unknown Job") == "unknown job"


def test_etl_process(tmp_path):
    data = {
        "salary": ["10 triệu - 20 triệu", "trên 15 triệu", "Thỏa thuận"],
        "address": ["Hà Nội: Cầu Giấy", "Hồ Chí Minh", "Đà Nẵng: Hải Châu"],
        "job_title": ["Software Engineer", "Business Analyst", "Data Scientist"]
    }
    df = pd.DataFrame(data)
    csv_file = tmp_path / "/home/namngo/airflow/data/test_data.csv"
    df.to_csv(csv_file, index=False)

    processed_df = etl_process(csv_file)
    assert processed_df.shape[0] == 3
    assert "min_salary" in processed_df.columns
    assert "max_salary" in processed_df.columns
    assert "salary_unit" in processed_df.columns
    assert "city" in processed_df.columns
    assert "district" in processed_df.columns
    assert "normalized_job_title" in processed_df.columns
    assert processed_df.iloc[0]["min_salary"] == 10
    assert processed_df.iloc[1]["min_salary"] == 15
    assert processed_df.iloc[2]["salary_unit"] == None
