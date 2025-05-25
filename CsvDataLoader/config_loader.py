import json
import sys
import logging

logger = logging.getLogger(__name__)

def load_config(path='config.json'):
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load config file '{path}': {e}")
        sys.exit(1)