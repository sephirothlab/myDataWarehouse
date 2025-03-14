import pandas as pd


# Function to retrieve the schema of a BigQuery table
def get_bq_schema(client_bq, dataset_id,bigquery_table):
    # Get the table schema from BigQuery
    bigquery_table = f"{dataset_id}.{bigquery_table}"
    table = client_bq.get_table(bigquery_table)
    return table.schema

# Function to dynamically adjust DataFrame column types based on BigQuery schema
def adjust_dataframe_types(df, bq_schema):
    print(df)
    df = df.fillna(0)
    for field in bq_schema:
        column_name = field.name
        column_type = field.field_type

        # Ensure the DataFrame has the column in question
        if column_name in df.columns:
            if column_type == "STRING":
                df[column_name] = df[column_name].astype(str)
            elif column_type == "INTEGER":
                df[column_name] = df[column_name].astype(int)
            elif column_type == "FLOAT":
                df[column_name] = df[column_name].astype(float)
            elif column_type == "BOOLEAN":
                df[column_name] = df[column_name].astype(bool)
            elif column_type == "DATE":
                df[column_name] = pd.to_datetime(df[column_name]).dt.date
            elif column_type == "TIMESTAMP":
                df[column_name] = pd.to_datetime(df[column_name])
            # You can add more types (like NUMERIC, etc.) if needed
    return df