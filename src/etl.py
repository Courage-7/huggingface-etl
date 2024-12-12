import time
import requests
from pymongo import MongoClient
from datetime import datetime
from pyspark.sql import SparkSession
from src.utils import setup_logger
from src.config import MONGO_URI, DATABASE_NAME, COLLECTION_NAME, API_URL, LOG_FILE


class EnhancedHuggingFaceETL:
    def __init__(self):
        self.logger = setup_logger(LOG_FILE)
        self.spark = SparkSession.builder.appName("HuggingFaceETL").getOrCreate() # type: ignore
        self.mongo_client = MongoClient(MONGO_URI)
        self.db = self.mongo_client[DATABASE_NAME]
        self.collection = self.db[COLLECTION_NAME]
        self.error_count = 0
        self.processed_models = 0

    def extract_models(self, max_models=None, timeout=30):
        """
        Extract model data from HuggingFace API.
        """
        all_models = []
        url = API_URL

        try:
            while url and (max_models is None or len(all_models) < max_models):
                response = requests.get(url, timeout=timeout)
                response.raise_for_status()
                data = response.json()

                self.logger.info(f"Extracted {len(data)} models from {url}")
                all_models.extend(data)
                url = response.links.get('next', {}).get('url')

                time.sleep(1)  # Prevent API flooding
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error during extraction: {e}")
            self.error_count += 1
            raise

        return all_models[:max_models] if max_models else all_models

    def validate_model_data(self, models):
        """
        Validate and clean model data to include only required fields.
        """
        cleaned_models = []
        for model in models:
            try:
                if not model.get('modelId'):
                    self.logger.warning(f"Skipping model without ID: {model}")
                    continue

                # Extract only the required fields
                cleaned_model = {
                    "modelId": model.get("modelId"),
                    "license": model.get("license"),
                    "pipeline_tag": model.get("pipeline_tag"),
                    "evaluation": model.get("evaluation"),
                    "memory_requirements": model.get("memory_requirements"),
                    "parsed_date": datetime.now().strftime("%Y-%m-%d"),
                    "parsed_datetime": datetime.now().isoformat(),
                    "createdAt": model.get("createdAt"),
                    "downloads": model.get("downloads"),
                    "likes": model.get("likes"),
                }

                # Remove None values
                cleaned_model = {k: v for k, v in cleaned_model.items() if v is not None}

                cleaned_models.append(cleaned_model)
            except Exception as e:
                self.logger.error(f"Validation error: {e}")
                self.error_count += 1

        return cleaned_models

    def _process_batch(self, batch):
        """
        Insert a batch of models into MongoDB.
        """
        self.logger.info(f"Inserting batch of size {len(batch)} into MongoDB")
        try:
            self.collection.insert_many(batch, ordered=False)
        except Exception as e:
            self.logger.error(f"Error during MongoDB insertion: {e}")
            self.error_count += 1

    def execute_etl(self, max_models=10000, batch_size=100):
        """
        Execute the ETL process.
        """
        start_time = datetime.now()
        self.logger.info("Starting ETL process")

        try:
            raw_models = self.extract_models(max_models)
            validated_models = self.validate_model_data(raw_models)

            for i in range(0, len(validated_models), batch_size):
                batch = validated_models[i:i + batch_size]
                self._process_batch(batch)
                self.processed_models += len(batch)
                self.logger.info(f"Processed batch {i // batch_size + 1}")

        except Exception as e:
            self.logger.error(f"ETL process failed: {e}")

        finally:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            self.logger.info(f"""
            ETL Summary:
            - Total Models Processed: {self.processed_models}
            - Errors Encountered: {self.error_count}
            - Duration: {duration} seconds
            """)


def main():
    etl = EnhancedHuggingFaceETL()
    etl.execute_etl(max_models=1000)


if __name__ == "__main__":
    main()
