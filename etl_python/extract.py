import pandas as pd
import logging

logging.basicConfig(
    filename='logging.log',
    level= logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s'
)

def extract(file_path):
    try:
        logging.info('-------------------------------------------------------------')
        logging.info('EXTRACTING THE DATA\n')
        logging.info(f"Extracting Data from file path {file_path}")
        df = pd.read_csv(file_path)
        logging.info(f"Extracted {len(df)} rows")
        return df
    
    except Exception as e:
        logging.error(f"Extraction Failed : {e}")
        raise
