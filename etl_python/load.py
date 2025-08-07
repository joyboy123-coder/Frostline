import os
import logging
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

def load(csv_file):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        insecure_mode = True
    )

    cursor = conn.cursor()

    try:
        stage = os.getenv("STAGE_NAME")
        table = os.getenv("TABLE_NAME")
        abs_path = os.path.abspath(csv_file)

        cursor.execute(f"PUT file://{abs_path} @{stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        
        cursor.execute(f"""
            COPY INTO {table}
            FROM @{stage}/{os.path.basename(csv_file)}
            FILE_FORMAT = (type = 'CSV' field_optionally_enclosed_by='"' skip_header=1)
            PURGE = TRUE;
        """)
        
        logging.info('✅ DATA LOADING TO SNOWFLAKE COMPLETED')

    except Exception as e:
        logging.error(f"❌ Exception Failed: {e}")

    finally:
        cursor.close()
        conn.close()
