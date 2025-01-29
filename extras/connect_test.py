from sqlalchemy import create_engine, text

# Corrected connection URL
db_url = "mysql+pymysql://app_user:Test_Pass%40123@localhost:3306/my_db"
engine = create_engine(db_url)

try:
    with engine.connect() as connection:
        # Use `text` to execute raw SQL queries
        result = connection.execute(text("SELECT 1"))
        print("Connection successful:", result.scalar())
except Exception as e:
    print("Connection failed:", e)
