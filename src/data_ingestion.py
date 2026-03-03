import os
import sys
from dataclasses import dataclass

from sklearn.model_selection import train_test_split

from src.exception_handler import CustomException
from src.logger import get_logger
from src.utils import read_sql_data

logger = get_logger(__name__)


@dataclass
class DataIngestionConfig:
    artifacts_dir: str = "artifacts"
    raw_data_path: str = os.path.join("artifacts", "raw.csv")
    train_data_path: str = os.path.join("artifacts", "train.csv")
    test_data_path: str = os.path.join("artifacts", "test.csv")


class DataIngestion:
    def __init__(self):
        self.config = DataIngestionConfig()

    def initiate_data_ingestion(self):
        try:
            logger.info("Starting data ingestion from MySQL")

            query = """
                SELECT 
                    c.customer_id,
                    c.first_name,
                    c.last_name,
                    c.email,
                    c.active,
                    IFNULL(SUM(p.amount), 0) AS total_spent,
                    IFNULL(COUNT(p.payment_id), 0) AS total_transactions
                FROM customer c
                LEFT JOIN payment p 
                ON c.customer_id = p.customer_id
                GROUP BY c.customer_id;
            """

            df = read_sql_data(query)

            logger.info(f"Data successfully read → Shape: {df.shape}")

            os.makedirs(self.config.artifacts_dir, exist_ok=True)

            df.to_csv(self.config.raw_data_path, index=False)
            logger.info("Raw data saved")

            train_set, test_set = train_test_split(
                df,
                test_size=0.2,
                random_state=42
            )

            train_set.to_csv(self.config.train_data_path, index=False)
            test_set.to_csv(self.config.test_data_path, index=False)

            logger.info("Train-test split completed")

            return (
                self.config.train_data_path,
                self.config.test_data_path
            )

        except Exception as e:
            logger.error("Error in data ingestion")
            raise CustomException(e, sys)