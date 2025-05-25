import pandas as pd
import logging
import sys

logger = logging.getLogger(__name__)

def read_csv_in_chunks(csv_path, batch_size):
    try:
        return pd.read_csv(csv_path, chunksize=batch_size, dtype=str)
    except Exception as e:
        logger.error(f"Failed to read CSV file: {e}")
        sys.exit(1)