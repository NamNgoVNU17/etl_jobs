from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

DB_URL = "postgresql://postgres:password@localhost:5432/my_db"
try:
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        print("✅ Kết nối PostgreSQL thành công!")
except OperationalError as e:
    print(f"❌ Lỗi kết nối PostgreSQL: {e}")
