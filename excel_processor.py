# excel_processor.py

import pandas as pd
from .utils import rename_column_if_keyword

def process_excel(input_excel, json_folder):
    # Read Excel file
    df = pd.read_excel(input_excel, engine="openpyxl", sheet_name="Ingestion Details")

    # Debug: Print column names to verify
    print("Columns in the Excel file:")
    print(df.columns)

    # Strip any leading/trailing spaces from column names
    df.columns = df.columns.str.strip()

    # Debug: Print column names after stripping spaces
    print("Columns after stripping spaces:")
    print(df.columns)

    # Replace multiple spaces with a single space (in case of hidden characters)
    df.columns = df.columns.str.replace(r'\s+', ' ', regex=True)

    # Debug: Print column names after replacing spaces
    print("Columns after replacing spaces:")
    print(df.columns)

    # Convert column names to lowercase for case-insensitive access
    df.columns = df.columns.str.lower()

    # Debug: Print column names after converting to lowercase
    print("Columns after converting to lowercase:")
    print(df.columns)

    return df