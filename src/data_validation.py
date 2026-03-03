import os
import sys
import pandas as pd
import yaml
from src.exception_handler import CustomException
from src.logger import get_logger

logger = get_logger(__name__)


class DataValidation:

    def __init__(self, train_path, config_path="config/config.yaml"):
        self.train_path = train_path
        self.config_path = config_path

    def read_config(self):
        try:
            with open(self.config_path, "r") as file:
                return yaml.safe_load(file)
        except Exception as e:
            raise CustomException(e, sys)

    def validate_columns(self, df, expected_columns):
        logger.info("Validating columns")

        missing_cols = [
            col for col in expected_columns if col not in df.columns
        ]

        if missing_cols:
            logger.error(f"Missing columns: {missing_cols}")
            return False

        logger.info("All required columns are present")
        return True

    def validate_nulls(self, df):
        logger.info("Checking for null values")

        null_percent = df.isnull().mean() * 100

        high_null_cols = null_percent[null_percent > 30]

        if not high_null_cols.empty:
            logger.warning(f"High null percentage columns: {high_null_cols}")
            return False

        logger.info("Null value check passed")
        return True

    def validate_duplicates(self, df):
        logger.info("Checking duplicate rows")

        duplicates = df.duplicated().sum()

        if duplicates > 0:
            logger.warning(f"Duplicate rows found: {duplicates}")
            return False

        logger.info("No duplicate rows found")
        return True

    def initiate_validation(self):
        try:
            logger.info("Starting data validation")

            df = pd.read_csv(self.train_path)
            config = self.read_config()

            col_status = self.validate_columns(df, config["columns"])
            null_status = self.validate_nulls(df)
            dup_status = self.validate_duplicates(df)

            if col_status and null_status and dup_status:
                logger.info("Data validation successful")
                return True

            logger.error("Data validation failed")
            return False

        except Exception as e:
            raise CustomException(e, sys)