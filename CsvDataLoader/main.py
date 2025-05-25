import sys

from config_loader import load_config
from mongo_connector import connect_to_mongo
from csv_reader import read_csv_in_chunks
from data_inserter import insert_data_to_mongo
import logging

logger = logging.getLogger(__name__)

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

    logger.info("Starting CSV import")
    collection = connect_to_mongo(MONGO_URI, DATABASE, COLLECTION)
    chunks = read_csv_in_chunks(CSV_PATH, BATCH_SIZE)
    total_inserted = insert_data_to_mongo(chunks, collection)

    logger.info(f"CSV import completed. Total inserted: {total_inserted}")

if __name__ == "__main__":
    main()