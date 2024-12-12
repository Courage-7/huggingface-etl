## HuggingFace ETL Pipeline
So, this project is an Extract, Transform, Load (ETL) pipeline designed to extract model metadata from the Hugging Face API, process the data, and load it into a MongoDB collection.

## Features

Data Extraction: Fetches model metadata from the Hugging Face API.

Data Validation: Cleans and validates the extracted data.

Data Loading: Inserts the transformed data into a MongoDB collection.

Error Handling: Includes robust logging and retries for API requests.

Batch Processing: Processes data in batches for scalability.

## Requirements
### System Dependencies
- Python 3.8+
- MongoDB
- Apache Spark

## Configuration
MONGO_URI=mongodb://localhost:27017

MONGO_DATABASE=huggingface_etl

MONGO_COLLECTION=models


### Installation
Install project dependencies with:
pip install -r requirements.txt

### Running
run script as a module relative to the project root ( python -m src.etl )
if running the etl ( python -m src.etl ) flags an error.





# ADIOS!
=======
# huggingface-etl
>>>>>>> 83bd33e3691ee72d017007fe94da2b05e24f48f8
