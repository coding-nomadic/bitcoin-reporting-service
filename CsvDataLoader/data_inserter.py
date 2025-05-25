import numpy as np
from tqdm import tqdm
import logging

logger = logging.getLogger(__name__)

def insert_data_to_mongo(chunks, collection):
    total_inserted = 0

    for i, chunk in enumerate(tqdm(chunks, desc="Processing chunks")):
        chunk = chunk.replace({np.nan: None})
        records = chunk.to_dict(orient="records")

        if records:
            try:
                collection.insert_many(records, ordered=False)
                total_inserted += len(records)
                logger.info(f"Chunk {i+1}: Inserted {len(records)} records. Total so far: {total_inserted}")
            except Exception as e:
                logger.warning(f"Chunk {i+1}: Insert error: {e}")

    return total_inserted