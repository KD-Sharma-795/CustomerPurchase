import os
import sys
import pandas as pd
import pymysql

from src.exception_handler import CustomException
from src.logger import logging
from dotenv import load_dotenv

# Load environment variables (override system vars)
load_dotenv(override=True)

host = os.getenv("host")
user = os.getenv("username")
password = os.getenv("password")
database = os.getenv("db_name")


def read_sql_data(query: str) -> pd.DataFrame:
    """
    Read data from MySQL database using given SQL query
    """
    logging.info("Connecting to MySQL database")

    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        logging.info("Connection established successfully")

        df = pd.read_sql(query, connection)

        logging.info(f"Data fetched successfully → shape: {df.shape}")
        print("DB CONFIG →", host, user, database)

        connection.close()
        return df

    except Exception as e:
        logging.error("Error while reading data from MySQL")
        raise CustomException(e, sys)