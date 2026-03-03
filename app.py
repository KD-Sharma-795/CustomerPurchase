import sys
from src.logger import get_logger
from src.data_validation import DataValidation
from src.data_transformation import DataTransformation
from src.exception_handler import CustomException
from src.data_ingestion import DataIngestion

logger = get_logger(__name__)


def main():
    try:
        logger.info("🚀 Application started")
        print("🚀 ML Pipeline Started")

        
        # 1️⃣ Data Ingestion
        
        ingestion = DataIngestion()
        train_path, test_path = ingestion.initiate_data_ingestion()

        logger.info(f"Train data saved at: {train_path}")
        logger.info(f"Test data saved at: {test_path}")

        print("✅ Data Ingestion Completed Successfully")

        
        # 2️⃣ Data Validation
        
        validator = DataValidation(train_path)
        validation_status = validator.initiate_validation()

        if not validation_status:
            print("⚠️ Validation issues found. Starting Data Transformation...")
            logger.warning("Validation issues found. Running transformation step.")

            
            # 3️⃣ Data Transformation (Cleaning)
            
            transformer = DataTransformation(train_path)
            cleaned_path = transformer.initiate_transformation()

            logger.info(f"Cleaned data saved at: {cleaned_path}")
            print("✅ Data Cleaning Completed Successfully")

        else:
            print("✅ Data Validation Completed Successfully")
            logger.info("Data Validation Completed Successfully")

        print("🎯 Pipeline executed successfully")

    except Exception as e:
        logger.exception("Exception occurred in application")
        raise CustomException(e, sys)


if __name__ == "__main__":
    main()