import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime


class DataIngestion:
    def __init__(self):
        # MySQL connection config
        self.username = "root"
        self.password = "your_password"
        self.host = "localhost"
        self.port = "3306"
        self.database = "sakila"

        # output folder
        self.raw_data_path = "data/raw"
        os.makedirs(self.raw_data_path, exist_ok=True)

    def connect_db(self):
        """Create MySQL connection using SQLAlchemy"""
        connection_string = (
            f"mysql+pymysql://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
        engine = create_engine(connection_string)
        return engine

    def fetch_data(self):
        """
        Example: Customer + Payment join
        You can modify query as per project need
        """
        query = """
        SELECT 
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email,
            c.active,
            SUM(p.amount) AS total_spent,
            COUNT(p.payment_id) AS total_transactions
        FROM customer c
        LEFT JOIN payment p 
        ON c.customer_id = p.customer_id
        GROUP BY c.customer_id;
        """

        engine = self.connect_db()
        df = pd.read_sql(query, engine)

        return df

    def save_data(self, df):
        """Save dataset to data/raw folder"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(self.raw_data_path, f"customer_data_{timestamp}.csv")
        df.to_csv(file_path, index=False)
        print(f"✅ Data saved at: {file_path}")

    def log_shape(self, df):
        """Log dataset shape"""
        print("📊 Dataset Info")
        print("Rows:", df.shape[0])
        print("Columns:", df.shape[1])
        print("Columns List:", list(df.columns))


if __name__ == "__main__":
    ingestion = DataIngestion()
    data = ingestion.fetch_data()
    ingestion.log_shape(data)
    ingestion.save_data(data)