import os
import sys
import pandas as pd
import pymysql

from dotenv import load_dotenv
from src.exception_handler import CustomException
from src.logger import get_logger

logger = get_logger(__name__)

# Load environment variables
load_dotenv()

host = os.getenv("host")
user = os.getenv("username")
password = os.getenv("password")
database = os.getenv("db_name")


def read_sql_data(query: str) -> pd.DataFrame:
    """
    Read data from MySQL database using given SQL query
    """

    try:
        logger.info("Connecting to MySQL database")

        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            cursorclass=pymysql.cursors.DictCursor
        )

        logger.info("Connection established successfully")

        df = pd.read_sql(query, connection)

        logger.info(f"Data fetched successfully → Shape: {df.shape}")

        connection.close()
        logger.info("Database connection closed")

        return df

    except Exception as e:
        logger.error("Error while reading data from MySQL")
        raise CustomException(e, sys)