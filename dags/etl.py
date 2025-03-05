import pandas as pd
import re
from sqlalchemy import create_engine
from datetime import datetime

def normal_salary(s):
    s = str(s).lower()
    donvi = "USD" if "usd" in s else "VND" if "triệu" in s else None

    if "thỏa thuận" in s:
        return None, None, donvi

    temp = re.search(r"trên\s*([\d,]+)", s)
    if temp:
        min_salary = int(temp.group(1).replace(",",""))
        return min_salary, None, donvi

    temp = re.search(r"tới\s*([\d,]+)", s)
    if temp:
        max_salary = int(temp.group(1).replace(",",""))
        return None, max_salary, donvi

    temp = re.search(r"([\d,]+)\s*-\s*([\d,]+)", s)
    if temp:
        min_salary = int(temp.group(1).replace(",",""))
        max_salary = int(temp.group(2).replace(",",""))
        return min_salary, max_salary, donvi

    temp = re.search(r"([\d,]+)", s)
    if temp:
        salary = int(temp.group(1).replace(",",""))
        return salary, salary, donvi

    return None, None, donvi

def process_address(s):
    parts = str(s).split(": ")
    if len(parts) >= 2:
        return parts[0], parts[1]
    if len(parts) == 1:
        return parts[0], None

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

def etl_process(csv_path):
    # Đọc dữ liệu từ CSV
    df = pd.read_csv(csv_path)

    # Xử lý lương
    df[['min_salary', 'max_salary', 'salary_unit']] = df['salary'].apply(lambda x: pd.Series(normal_salary(x)))

    # Xử lý địa chỉ
    df[['city', 'district']] = df['address'].apply(lambda x: pd.Series(process_address(x)))

    # Chuẩn hóa job title
    df['normalized_job_title'] = df['job_title'].apply(nor_job_title)

    # Thêm cột timestamp
    df['processed_at'] = datetime.now()

    return df

def load_to_postgres(df, table_name, connection_string):
    # Tạo engine kết nối
    engine = create_engine(connection_string)

    try:
        # Tải dữ liệu vào PostgreSQL
        with engine.connect() as connection:
            df.to_sql(
                table_name,
                connection,
                if_exists="replace",
                index=False
            )
        print(f"Dữ liệu đã được tải vào bảng {table_name} thành công!")
    except Exception as e:
        print(f"Lỗi khi tải dữ liệu: {e}")

def main():
    # Đường dẫn file CSV
    csv_path = '/home/namngo/project/prjUnigap1/data/data.csv'

    # Chuỗi kết nối PostgreSQL
    connection_string = "postgresql+psycopg2://postgres:password@localhost:5432/my_db"

    # Bảng đích
    table_name = "job_data"

    # Thực hiện ETL
    processed_df = etl_process(csv_path)

    # Tải dữ liệu vào PostgreSQL
    load_to_postgres(processed_df, table_name, connection_string)

if __name__ == "__main__":
    main()