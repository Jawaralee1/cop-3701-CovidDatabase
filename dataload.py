

import oracledb
import csv
from pathlib import Path


# --- SETUP ---
LIB_DIR = r"C:\Users\boltm\Documents\instantclient_11_2"


# Your Oracle Credentials
DB_USER = "JAWARALEE1_SCHEMA_KDMD1" # or your FreeSQL username
DB_PASS = "#WIG28BPF3PPQ9CW4VJ5cCGHCMA7O8" # your password for the dbms user
DB_DSN  = "db.freesql.com" + ":" + "1521" + "/" + "19c_fy05s" # or your FreeSQL DSN

path = r"C:\Users\boltm\Documents\data\dfWHO_REGION.csv"
provPath = r"C:\Users\boltm\Documents\data\dfProvider.csv"
countryPath = r"C:\Users\boltm\Documents\data\dfCountry.csv"
reportPath = r"C:\Users\boltm\Documents\data\dfReport.csv"
case_dataPath = r"C:\Users\boltm\Documents\data\dfCase_Data.csv"
# Initialize Thick Mode (Required for FreeSQL/Cloud)
oracledb.init_oracle_client(lib_dir=LIB_DIR)

def bulk_load_csv_region(file_path):
    try:
        # 1. Connect
        conn = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)
        cursor = conn.cursor()
        
        # 2. Read CSV Data into a List
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row
            data_to_insert = [row for row in reader]

        # 3. Prepare Bulk Insert SQL
        # :1 and :2 correspond to the values in each row of your list
        sql = "INSERT INTO WHO_REGION(REGION_CODE, REGION_NAME) VALUES (:1, :2)"

        # 4. Execute Batch
        print(f"Starting bulk load of {len(data_to_insert)} rows...")
        cursor.executemany(sql, data_to_insert)
        
        # 5. Commit Changes
        conn.commit()
        print(f"Successfully loaded {cursor.rowcount} rows into the database.")

    except Exception as e:
        print(f"Error during bulk load: {e}")
        if 'conn' in locals():
            conn.rollback() # Undo changes if an error occurs

    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

def bulk_load_csv_report(file_path):
    try:
        # 1. Connect
        conn = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)
        cursor = conn.cursor()
        
        # 2. Read CSV Data into a List
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row
            data_to_insert = [row for row in reader]

        # 3. Prepare Bulk Insert SQL
        # :1 and :2 correspond to the values in each row of your list
        sql = "INSERT INTO REPORT(REPORT_ID, PROV_ID, REPORT_DATE, DATA_ID) VALUES (:1, :2, to_date(:3, 'MM/DD/YYYY'), :4)"

        # 4. Execute Batch
        print(f"Starting bulk load of {len(data_to_insert)} rows...")
        cursor.executemany(sql, data_to_insert)
        
        # 5. Commit Changes
        conn.commit()
        print(f"Successfully loaded {cursor.rowcount} rows into the database.")

    except Exception as e:
        print(f"Error during bulk load: {e}")
        if 'conn' in locals():
            conn.rollback() # Undo changes if an error occurs

    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

def bulk_load_csv_provider(file_path):
    try:
        # 1. Connect
        conn = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)
        cursor = conn.cursor()
        
        # 2. Read CSV Data into a List
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row
            data_to_insert = [row for row in reader]

        # 3. Prepare Bulk Insert SQL
        # :1 and :2 correspond to the values in each row of your list
        sql = "INSERT INTO PROVIDER(PROV_ID, PROV_NAME) VALUES (:1, :2)"

        # 4. Execute Batch
        print(f"Starting bulk load of {len(data_to_insert)} rows...")
        cursor.executemany(sql, data_to_insert)
        
        # 5. Commit Changes
        conn.commit()
        print(f"Successfully loaded {cursor.rowcount} rows into the database.")

    except Exception as e:
        print(f"Error during bulk load: {e}")
        if 'conn' in locals():
            conn.rollback() # Undo changes if an error occurs

    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

def bulk_load_csv_country(file_path):
    try:
        # 1. Connect
        conn = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)
        cursor = conn.cursor()
        
        # 2. Read CSV Data into a List
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row
            data_to_insert = [row for row in reader]

        # 3. Prepare Bulk Insert SQL
        # :1 and :2 correspond to the values in each row of your list
        sql = "INSERT INTO COUNTRY (COUNTRY_ID, REGION_CODE, COUNTRY_NAME, PROV_ID) VALUES (:1, :2, :3, :4)"



        # 4. Execute Batch
        print(f"Starting bulk load of {len(data_to_insert)} rows...")
        cursor.executemany(sql, data_to_insert)
        
        # 5. Commit Changes
        conn.commit()
        print(f"Successfully loaded {cursor.rowcount} rows into the database.")

    except Exception as e:
        print(f"Error during bulk load: {e}")
        if 'conn' in locals():
            conn.rollback() # Undo changes if an error occurs

    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

def bulk_load_csv_case_data(file_path):
    try:
        # 1. Connect
        conn = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)
        cursor = conn.cursor()

        # 2. Read CSV Data into a List
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row
            data_to_insert = [row for row in reader]

        # 3. Prepare Bulk Insert SQL
        # :1 and :2 correspond to the values in each row of your list
        sql = "INSERT INTO CASE_DATA (DATA_ID, CONFIRMED, DEATHS, RECOVERED, ACTIVE) VALUES (:1, :2, :3, :4, :5)"



        # 4. Execute Batch
        print(f"Starting bulk load of {len(data_to_insert)} rows...")
        cursor.executemany(sql, data_to_insert)

        # 5. Commit Changes
        conn.commit()
        print(f"Successfully loaded {cursor.rowcount} rows into the database.")

    except Exception as e:
        print(f"Error during bulk load: {e}")
        if 'conn' in locals():
            conn.rollback() # Undo changes if an error occurs

    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()


# Run the function
#bulk_load_csv_region(path)
#bulk_load_csv_country(countryPath)
#bulk_load_csv_provider(provPath)
bulk_load_csv_report(reportPath)
#bulk_load_csv_case_data(case_dataPath)