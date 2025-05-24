import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm
import numpy as np
import logging
import json
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def load_config(path='config.json'):
    try:
        with open(path, 'r') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.error(f"Failed to load config file '{path}': {e}")
        sys.exit(1)

def main():
    config = load_config()

    try:
        MONGO_URI = config['mongo']['uri']
        DATABASE = config['mongo']['database']
        COLLECTION = config['mongo']['collection']
        CSV_PATH = config['csv']['path']
        BATCH_SIZE = int(config['csv']['batch_size'])
    except KeyError as e:
        logger.error(f"Missing config key: {e}")
        sys.exit(1)

    logger.info(f"Starting CSV import")
    logger.info(f"MongoDB URI: {MONGO_URI}")
    logger.info(f"Database: {DATABASE}, Collection: {COLLECTION}")
    logger.info(f"CSV Path: {CSV_PATH}, Batch Size: {BATCH_SIZE}")

    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE]
        collection = db[COLLECTION]
        logger.info("Connected to MongoDB successfully")
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        sys.exit(1)

    total_inserted = 0

    try:
        chunks = pd.read_csv(CSV_PATH, chunksize=BATCH_SIZE, dtype=str)
    except Exception as e:
        logger.error(f"Failed to read CSV file: {e}")
        sys.exit(1)

    for i, chunk in enumerate(tqdm(chunks, desc="Processing chunks")):
        chunk = chunk.replace({np.nan: None})  # Replace NaN with None for MongoDB compatibility
        records = chunk.to_dict(orient="records")

        if records:
            try:
                collection.insert_many(records, ordered=False)
                total_inserted += len(records)
                logger.info(f"Chunk {i+1}: Inserted {len(records)} records. Total so far: {total_inserted}")
            except Exception as e:
                logger.warning(f"Chunk {i+1}: Insert error: {e}")

    logger.info(f"CSV import completed. Total inserted: {total_inserted}")

if __name__ == "__main__":
    main()
