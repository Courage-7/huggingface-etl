## HuggingFace ETL Pipeline
This project is an Extract, Transform, Load (ETL) pipeline designed to extract model metadata from the Hugging Face API, process the data, and load it into a MongoDB collection.

## Features
- **Data Extraction:** Fetches model metadata from the Hugging Face API.
- **Data Validation:** Cleans and validates the extracted data.
- **Data Loading:** Inserts the transformed data into a MongoDB collection.
- **Error Handling:** Includes robust logging and retries for API requests.
- **Batch Processing:** Processes data in batches for scalability.

## Project Structure
```bash
courage-7-huggingface-etl/
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── etl.py
│   ├── utils.py
│   └── __pycache__/
├── tests/
│   ├── test_data_validation.py
│   └── tests_etl.py
├── README.md
└── requirements.txt
```

## Requirements
System Dependencies
- Python 3.8+
- MongoDB
- Apache Spark
- PySpark

## Configuration
Set the following in your .env file or environment variables:
```
MONGO_URI=mongodb+srv://<username>:<password>@<cluster>.mongodb.net/
MONGO_DATABASE=huggingface_etl
MONGO_COLLECTION=models
```

## Installation
Install project dependencies with:
```
pip install -r requirements.txt
```

## Running
Run the ETL script as a module from the project root:
```
python -m src.etl
```
If this flags an error, make sure you're running from the root directory (courage-7-huggingface-etl/).
