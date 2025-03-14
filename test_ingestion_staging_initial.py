import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from io import StringIO
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime


# โหลดค่าจากไฟล์ .env
load_dotenv()

# สร้างการเชื่อมต่อกับ Snowflake
conn = snowflake.connector.connect(
    user= os.getenv("user"),
    password= os.getenv("password"),
    account= os.getenv("account"),
    warehouse= os.getenv("warehouse"),
    database= os.getenv("database"),
    schema= os.getenv("schema")
)

snowflake_schema = 'TPCDS_SF10TCL'



# กำหนด path ของไฟล์ Service Account Key
service_account_path = os.getenv("service_account_path")

# สร้าง Credentials จากไฟล์ Service Account
credentials = service_account.Credentials.from_service_account_file(service_account_path)

# สร้าง client สำหรับเชื่อมต่อกับ BigQuery โดยใช้ credentials ที่กำหนดเอง
client_bq = bigquery.Client(credentials=credentials, project=credentials.project_id)
client_gcs = storage.Client(credentials=credentials, project=credentials.project_id)


# Google Cloud Storage and BigQuery settings
gcs_bucket_name = 'ojsnd-data-landing-zone'
gcs_folder = 'snowflake'  
project_id = credentials.project_id
dataset_id = 'wh_staging'

# Function to export data from Snowflake to GCS as Parquet
def export_table_to_gcs_as_parquet(table_name):
    # Construct the filename with the table name and export date
    export_date = datetime.now().strftime("%Y%m%d")
    file_name = f"{table_name}_{export_date}.parquet"
    file_path = f"{gcs_folder}/{file_name}"

    # Query to get the first 100 rows of the table
    query = f"SELECT * FROM {table_name} LIMIT 100"
    cursor = conn.cursor()
    cursor.execute(query)

    # Fetch the data into a pandas DataFrame
    df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

    # Convert the DataFrame to a Parquet file using PyArrow
    table = pa.Table.from_pandas(df)
    pq.write_table(table, '/tmp/' + file_name)

    # Upload the Parquet file to Google Cloud Storage
    bucket = client_gcs.get_bucket(gcs_bucket_name)
    blob = bucket.blob(file_path)
    blob.upload_from_filename('/tmp/' + file_name)

    print(f"Exported {table_name} to GCS: {file_path}")
    cursor.close()

# Function to load data from GCS to BigQuery
def load_gcs_to_bq(table_name):
    # Construct the GCS file path and the BigQuery table reference
    export_date = datetime.now().strftime("%Y%m%d")
    file_name = f"{table_name}_{export_date}.parquet"
    gcs_file_path = f"gs://{gcs_bucket_name}/{gcs_folder}/{file_name}"

    # Define the BigQuery table reference
    table_ref = client_bq.dataset(dataset_id).table(table_name)

    # Load the Parquet data from GCS into BigQuery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
    )

    load_job = client_bq.load_table_from_uri(
        gcs_file_path, table_ref, job_config=job_config
    )

    load_job.result()  # Wait for the job to complete
    print(f"Loaded {file_name} into BigQuery table {table_name}.")

# Function to get all table names from Snowflake
def get_all_tables_from_snowflake():
    # Query to get all table names in the specified schema
    query = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{snowflake_schema}'"
    cursor = conn.cursor()
    cursor.execute(query)

    tables = cursor.fetchall()
    cursor.close()

    return [table[0] for table in tables]

# Main workflow
def main():
    # Get the list of tables from Snowflake
    tables = get_all_tables_from_snowflake()

    for table in tables:
        # Export each table to GCS as Parquet
        export_table_to_gcs_as_parquet(table)

        # Load the exported data from GCS to BigQuery
        load_gcs_to_bq(table)

    # Close the Snowflake connection
    conn.close()

# Run the main function
main()