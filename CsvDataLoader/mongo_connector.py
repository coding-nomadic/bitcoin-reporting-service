from pymongo import MongoClient
import sys
import logging

logger = logging.getLogger(__name__)

def connect_to_mongo(uri, database, collection):
    try:
        client = MongoClient(uri)
        db = client[database]
        coll = db[collection]
        logger.info("Connected to MongoDB successfully")
        return coll
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        sys.exit(1)