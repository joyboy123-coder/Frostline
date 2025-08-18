# ❄️ Frostline ETL Pipeline

A simple end-to-end ETL pipeline that:
- 📥 Extracts data from a local CSV (`data/raw_data.csv`)
- 🧹 Transforms and cleans the dataset
- 💾 Saves a cleaned CSV (`data/cleaned_data.csv`)
- 🚀 Loads the cleaned data into Snowflake ❄️ using an internal stage and `COPY INTO`

## 🗂️ Project structure
- 🧠 `etl_pipeline.py`: Orchestrates the ETL flow
- 📦 `etl_python/extract.py`: Reads the raw CSV and logs metadata
- 🧹 `etl_python/transform.py`: Cleans/transforms columns and standardizes schema
- ⬆️ `etl_python/load.py`: Loads the cleaned CSV into Snowflake (PUT + COPY INTO)
- 📂 `data/`: Input and output CSV files
- 🧾 `sql_schema_table.sql`: Snowflake stage/table DDL
- 📝 `logging.log`: Pipeline log file

## 🧰 Prerequisites
- Python 3.10+ (Windows/macOS/Linux)
- Access to a Snowflake account and permissions to create/use stage, database, schema, and table

## ⚙️ Setup
1) Create and activate a virtual environment
```bash
python -m venv .venv
.venv\\Scripts\\activate   # Windows PowerShell
# source .venv/bin/activate  # macOS/Linux
```

2) Install dependencies
```bash
pip install -r requirements.txt
```

3) Prepare input data
- Place your source CSV at `data/raw_data.csv` (already present in this repo)
- The pipeline will write `data/cleaned_data.csv`

## ❄️ Snowflake configuration
Create a `.env` file at the project root with your credentials and object names:
```env
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_id   # e.g., abcd-xy12345
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=PEOPLE_DB
SNOWFLAKE_SCHEMA=PEOPLE_SCHEMA
SNOWFLAKE_ROLE=ACCOUNTADMIN

# Data load targets
STAGE_NAME=PEOPLE_STAGE
TABLE_NAME=PEOPLE_DB.PEOPLE_SCHEMA.PEOPLE
```

Then create the stage and table in Snowflake (once):
```sql
-- Run this in the Snowflake Worksheet using the configured DB/SCHEMA/ROLE
CREATE OR REPLACE STAGE PEOPLE_STAGE;

CREATE OR REPLACE TABLE PEOPLE_DB.PEOPLE_SCHEMA.PEOPLE(
    ID INT,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    AGE INT,
    SALARY FLOAT,
    JOINDATE DATE,
    ACTIVE STRING,
    DEPARTMENT STRING,
    EMAIL STRING,
    EXPERIENCE_YEARS INT,
    REMOTE BOOLEAN
);
```
(You can also run the same statements from `sql_schema_table.sql`.)

## ▶️ Run the pipeline
```bash
python etl_pipeline.py
```
What happens:
- 📥 Extract: reads `data/raw_data.csv`
- 🧹 Transform: cleans/standardizes columns (ID/Name/Age/Salary/JoinDate/Active/Department/Experience/Remote/Email)
- 💾 Output: writes `data/cleaned_data.csv`
- ❄️ Load: uploads the file to the internal stage and runs `COPY INTO` the target table

## 📝 Logging
- Logs are written to `logging.log` in the project root.

## 🛠️ Troubleshooting
- ❗ Missing env vars: ensure `.env` contains all required keys listed above
- 🔐 Snowflake errors on PUT/COPY: verify `STAGE_NAME`/`TABLE_NAME` and that your role has privileges
- 📦 Package import errors: run `pip install -r requirements.txt`
- 🧠 Large CSVs: the transform step loads into memory; ensure sufficient RAM

## 📌 Notes
- The loader uses `insecure_mode=True` for the Snowflake connector to simplify connectivity in some environments. Consider disabling it for production-grade security.