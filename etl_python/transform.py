import pandas as pd
import logging
import random
import numpy as np

def transform(df):
    try:
        logging.info("STARTING DATA CLEANING\n")
        logging.info("Loading dataset...")
        logging.info(f"Original dataset shape: {df.shape[0]} Rows and {df.shape[1]} Columns")
        
        df = df.drop_duplicates()
        logging.info(f"After dropping duplicates: {df.shape[0]} Rows and {df.shape[1]} Columns")

        df['ID'] = pd.to_numeric(df['ID'], errors='coerce')
        missing_id = df['ID'].isna().sum()
        if missing_id:
            logging.warning(f"Missing ID values: {missing_id}")
        df['ID'] = df['ID'].fillna(df['ID'].mean()).astype(int)
        df['ID'] = range(1, len(df) + 1)
        logging.info("Cleaned ID column.")

        df['Name'] = df['Name'].str.replace(r'[^a-zA-Z\s]', '', regex=True).str.replace(r'\s+', ' ', regex=True).str.title().str.strip()
        df['Name'] = df['Name'].apply(lambda x: random.choice(df['Name'].dropna().tolist()) if pd.isna(x) else x)
        df[['First Name', 'Last Name']] = df['Name'].str.split(' ', n=1, expand=True)
        logging.info("Cleaned Name, First Name, and Last Name.")


        df = df[['ID','First Name','Last Name','Age','Salary','JoinDate','Active','Department','Email','Experience(Years)','Remote']]


        df['Age'] = pd.to_numeric(df['Age'], errors='coerce')
        df['Age'] = df['Age'].apply(lambda x: x if 27 <= x <= 50 else np.nan)
        missing_age = df['Age'].isna().sum()
        df['Age'] = df['Age'].apply(lambda x: random.randint(27, 50) if pd.isna(x) else x).astype(int)
        logging.info(f"Filled {missing_age} invalid/missing Age values.")

        df['Salary'] = df['Salary'].str.replace('USD', '', regex=False)
        df['Salary'] = df['Salary'].apply(lambda x: float(x.replace('L', '')) * 100000 if isinstance(x, str) and 'L' in x else x)
        df['Salary'] = df['Salary'].apply(lambda x: float(x.replace('K', '')) * 10000 if isinstance(x, str) and 'K' in x else x)
        df['Salary'] = pd.to_numeric(df['Salary'], errors='coerce')
        missing_salary = df['Salary'].isna().sum()
        df['Salary'] = df['Salary'].apply(lambda x: random.randint(30000, 150000) if pd.isna(x) else x)
        logging.info(f"Cleaned Salary. Filled {missing_salary} values.")


        df['JoinDate'] = df['JoinDate'].replace('2023-13-40', np.nan)
        df['JoinDate'] = pd.to_datetime(df['JoinDate'], format='%Y-%m-%d', errors='coerce')
        null_count = df['JoinDate'].isna().sum()
        start_date = pd.to_datetime('2020-01-02')
        end_date = pd.to_datetime('2025-08-04')
        random_dates = pd.to_datetime(np.random.uniform(start_date.value, end_date.value, size=null_count))
        df.loc[df['JoinDate'].isna(), 'JoinDate'] = random_dates
        df['JoinDate'] = df['JoinDate'].dt.date
        logging.info(f"Fixed {null_count} missing JoinDate values.")


        df['Active'] = df['Active'].str.strip().str.title()
        df['Active'] = df['Active'].apply(lambda x: random.choice(['Yes','No']) if pd.isna(x) else x)

        def is_active(x):
            x = str(x).lower()
            if x in ['y','1','true']:
                return 'Yes'
            elif x in ['n','0','false']:
                return 'No'
            else:
                return x

        df['Active'] = df['Active'].apply(is_active).str.title()
        logging.info("Cleaned Active column.")

        df['Department'] = df['Department'].str.title().str.strip()
        df['Department'] = df['Department'].str.replace('R&D', 'IT')\
                                        .str.replace('Hr', 'HR')\
                                        .str.replace('Admin', 'Adminstration')
        df['Department'] = df['Department'].apply(lambda x: x if pd.notna(x) else random.choice(['IT','HR','Finance','Adminstration','Engineering']))
        logging.info("Cleaned Department column.")

        df['Experience(Years)'] = pd.to_numeric(df['Experience(Years)'], errors='coerce')
        missing_exp = df['Experience(Years)'].isna().sum()
        df['Experience(Years)'] = df['Experience(Years)'].apply(lambda x: random.randint(5, 10) if pd.isna(x) else x).astype(int)
        df.columns = df.columns.str.replace('Experience(Years)','Experience_Years')

        logging.info(f"Filled {missing_exp} missing Experience(Years) values.")

 
        df['Remote'] = df['Remote'].replace(['in-office', 'remote'], np.nan)

        def is_remote(x):
            if str(x).lower() in ['no','0','false']:
                return False
            elif str(x).lower() in ['yes','1','true']:
                return True
            return x

        df['Remote'] = df['Remote'].apply(is_remote)
        df['Remote'] = df['Remote'].apply(lambda x: random.choice([True, False]) if pd.isna(x) else x)
        logging.info("Cleaned Remote column.")

        df['Email'] = df.apply(
            lambda x: f"{x['First Name'].lower()}{x['Last Name'].lower()}{random.randint(1, 99)}@gmail.com",
            axis=1
        )
        logging.info("Generated Email addresses.")

   
        df.columns = df.columns.str.strip().str.upper().str.replace(' ', '_')

        logging.info("Standardized column names to uppercase.\n")


        logging.info("DATA CLEANING COMPLETE.")
        
        return df
    
    except Exception as e:
        logging.error(f'Exception Failed : {e}\n')

    finally:
        logging.info('-----------------------------------------------')


