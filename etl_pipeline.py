from etl_python.extract import extract
from etl_python.transform import transform
from etl_python.load import load
import logging
import os

def run_pipeline():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    raw_data = os.path.join(BASE_DIR, 'data', 'raw_data.csv')

    df = extract(raw_data)
    df = transform(df)

    output_file = os.path.join(BASE_DIR, 'data', 'cleaned_data.csv')
    df.to_csv(output_file,index = False)
 
    load(output_file)
    logging.info('========================================================')
    logging.info('ETL PIPELINE FINISHED SUCCESSFULLY :>')


if __name__ == "__main__":
    run_pipeline()