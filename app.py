import sys
from src.logger import get_logger
from src.exception_handler import CustomException
from src.data_ingestion import DataIngestion

logger = get_logger(__name__)


def main():
    try:
        logger.info("🚀 Application started")

        print("🚀 ML Pipeline Started")

        ingestion = DataIngestion()
        train_path, test_path = ingestion.initiate_data_ingestion()

        logger.info(f"Train data saved at: {train_path}")
        logger.info(f"Test data saved at: {test_path}")

        print("✅ Data Ingestion Completed Successfully")

    except Exception as e:
        logger.exception("Exception occurred in application")
        raise CustomException(e, sys)


if __name__ == "__main__":
    main()