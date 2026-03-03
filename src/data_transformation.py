import pandas as pd
from src.logger import get_logger

logger = get_logger(__name__)

class DataTransformation:

    def __init__(self, file_path):
        self.file_path = file_path

    def initiate_transformation(self):
        logger.info("Starting data transformation")

        df = pd.read_csv(self.file_path)

        # Remove duplicates
        duplicate_count = df.duplicated().sum()
        logger.warning(f"Removing {duplicate_count} duplicate rows")

        df = df.drop_duplicates()

        # Save cleaned file
        cleaned_path = "data/processed/cleaned_data.csv"
        df.to_csv(cleaned_path, index=False)

        logger.info("Data transformation completed")

        return cleaned_path