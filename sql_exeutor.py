from google.cloud import bigquery
from google.oauth2 import service_account

from dotenv import load_dotenv
import os


def run_bq_sql_from_folder(folder_path, project_id):
    """
    Loops through all SQL files in a folder and executes them in BigQuery.

    Args:
        folder_path (str): The folder containing SQL script files.
        project_id (str): The GCP project ID.

    Returns:
        None
    """
    client = bigquery.Client(project=project_id)

    # List all .sql files in the folder
    sql_files = [f for f in os.listdir(folder_path) if f.endswith(".sql")]

    for sql_filename in sql_files:
        sql_file_path = os.path.join(folder_path, sql_filename)

        # Read the SQL file
        with open(sql_file_path, "r") as file:
            sql_script = file.read()

        print(f"Executing SQL file: {sql_filename}...")

        # Run the SQL script
        query_job = client.query(sql_script)
        query_job.result()  # Wait for execution to complete

        print(f"✅ {sql_filename} executed successfully!")




if __name__ == "__main__":
    
    load_dotenv()
    service_account_path = os.getenv("service_account_path")
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    client_bq = bigquery.Client(credentials=credentials, project=credentials.project_id)

    dataset_conformed_id = 'wh_conformed'

    folder_path_conformed = "/path/to/sql/folder" 
    
    run_bq_sql_from_folder(folder_path_conformed, dataset_conformed_id, client_bq)
