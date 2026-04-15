'''
If you are running this code first time, and you don't have streamlit installed, then follow this instruction:
1. open a terminal
2. enter this command
    pip install streamlit
'''
import pandas as pd
import streamlit as st
import oracledb

# --- DATABASE SETUP ---
# Update this path to your local Instant Client folder
LIB_DIR = r"C:\oraclexe\app\oracle\instantclient_11_2\instantclient_23_0" # Your Instant Client Path
DB_USER = "SYSTEM"
DB_PASS = "QuiteME3M"
DB_DSN  = "127.0.0.1:1521/XE"

# Initialize Oracle Client for Thick Mode
@st.cache_resource
def init_db():
    if LIB_DIR:
        try:
            oracledb.init_oracle_client(lib_dir=LIB_DIR)
        except Exception as e:
            st.error(f"Error initializing Oracle Client: {e}")


init_db()


def get_connection():
    return oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)


# --- STREAMLIT UI ---
st.title("Global Covid-19 Surveillance Database")

menu = ["Average Region Death Rate", "Retrieve Region Data", "Retrieve Country Data", "Find Report Provider", "Retrieve Data From Provider"]
choice = st.sidebar.selectbox("Select Action", menu)

# --- Average Death Rate ---
if choice == "Average Region Death Rate":
    st.write("### Find a Region's Average Death Rate")
    region_name = st.text_input("Enter a Region Name (Case Sensitive)")
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT w.Region_Name, ROUND(AVG(d.Deaths/d.Confirmed),5) AS Avg_Death_Rate FROM WHO_REGION w JOIN COUNTRY c ON w.Region_Code = c.Region_Code JOIN PROVIDER p ON c.Prov_ID = p.Prov_ID JOIN REPORT r ON r.Report_ID = c.Country_ID JOIN CASE_DATA d ON d.Data_ID = r.Data_ID WHERE w.Region_Name = \'{region_name}\' GROUP BY w.Region_Name")
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=["Region_Name", "Avg_Death_Rate"])
        cur.close()
        conn.close()

        if data:
            st.table(df)
        else:
            st.info("No records found.")
    except Exception as e:
        st.error(f"Error: {e}")

# --- Retrieve Region Data ---
if choice == "Retrieve Region Data":
    st.write("### Retrieve Region Data")
    region_name = st.text_input("Enter a Region Name (Case Sensitive)")
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT w.Region_Name, c.Country_ID, c.Country_Name, d.Data_ID, d.Confirmed, d.Deaths, d.Recovered, d.Active, w.Region_Code, r.Report_ID, r.Report_Date, p.Prov_ID, p.Prov_Name FROM WHO_REGION w JOIN COUNTRY c ON w.Region_Code = c.Region_Code JOIN PROVIDER p ON c.Prov_ID = p.Prov_ID JOIN REPORT r ON r.Report_ID = c.Country_ID JOIN CASE_DATA d ON r.Data_ID = d.Data_ID WHERE w.Region_Name = \'{region_name}\'")
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=["Region_Name", "Country_ID", "Country_Name", "Data_ID", "Confirmed", "Deaths", "Recovered", "Active", "Region_Code", "Report_ID", "Report_Date", "Prov_ID", "Prov_Name"])
        cur.close()
        conn.close()

        if data:
            st.table(df)
        else:
            st.info("No records found.")
    except Exception as e:
        st.error(f"Error: {e}")

# --- Retrieve Country Data ---
if choice == "Retrieve Country Data":
    st.write("### Retrieve Country Data")
    country_name = st.text_input("Enter a Country Name (Case Sensitive)")
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT c.Country_Name, c.Country_ID, d.Data_ID, d.Confirmed, d.Deaths, d.Recovered, d.Active, w.Region_Name, w.Region_Code, r.Report_ID, r.Report_Date, p.Prov_ID, p.Prov_Name FROM WHO_REGION w JOIN COUNTRY c ON w.Region_Code = c.Region_Code JOIN PROVIDER p ON c.Prov_ID = p.Prov_ID JOIN REPORT r ON r.Report_ID = c.Country_ID JOIN CASE_DATA d ON r.Data_ID = d.Data_ID WHERE c.Country_Name = \'{country_name}\'")
        data = cur.fetchall()
        df = pd.DataFrame(data,columns=["Country_Name", "Country_ID", "Data_ID","Confirmed", "Deaths", "Recovered", "Active", "Region_Name", "Region_Code", "Report_ID", "Report_Date", "Prov_ID", "Prov_Name"])

        cur.close()
        conn.close()

        if data:
            st.table(df)
        else:
            st.info("No records found.")
    except Exception as e:
        st.error(f"Error: {e}")

# --- Find Report Provider ---
if choice == "Find Report Provider":
    st.write("### Find Report Provider")
    report_id = st.text_input("Enter a Report ID (Integer Values Only)",value="0", placeholder="0")
    try:
        int(report_id)
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT r.Report_ID, r.Prov_ID, p.Prov_Name FROM REPORT r JOIN PROVIDER p ON r.Prov_ID = p.Prov_ID WHERE r.Report_ID = {report_id}")
        data = cur.fetchall()
        df = pd.DataFrame(data,columns=["Report_ID", "Prov_ID", "Prov_Name"])
        cur.close()
        conn.close()

        if data:
            st.table(df)
        else:
            st.info("No records found.")
    except Exception as e:
        st.error(f"Error: {e}")

# --- Retrieve Data From Provider ---
if choice == "Retrieve Data From Provider":
    st.write("### Retrieve Data From Provider")
    provider_name = st.text_input("Enter a Provider Name (Case Sensitive)")
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT p.Prov_Name, c.Country_ID, c.Country_Name, d.Data_ID, d.Confirmed, d.Deaths, d.Recovered, d.Active, w.Region_Code, w.Region_Name, r.Report_ID, r.Report_Date, p.Prov_ID FROM PROVIDER p JOIN COUNTRY c ON p.Prov_ID = c.Prov_ID JOIN WHO_REGION w ON w.REGION_CODE = c.REGION_CODE JOIN REPORT r ON r.REPORT_ID = c.COUNTRY_ID JOIN CASE_DATA d ON r.DATA_ID = d.DATA_ID WHERE p.PROV_NAME = \'{provider_name}\'")
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=["Prov_Name", "Country_ID", "Country_Name", "Data_ID", "Confirmed", "Deaths","Recovered", "Active", "Region_Code", "Region_Name", "Report_ID", "Report_Date", "Prov_ID"])
        cur.close()
        conn.close()

        if data:
            st.table(df)
        else:
            st.info("No records found.")
    except Exception as e:
        st.error(f"Error: {e}")