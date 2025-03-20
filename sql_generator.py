# sql_generator.py

import os
import json

def generate_sql_file(json_file_path, columns, table_classification, is_pii, is_spii, pii_column_name, spii_column_name, srcl_vw_folder):
    """
    Generate a SQL file with a SELECT statement where each column is on a new line.
    If the table is classified as "Confidential" and has PII or SPII columns, add a CASE statement for those columns.
    """
    # Create the SQL file name (same as JSON file name but with .sql extension)
    sql_file_name = os.path.splitext(os.path.basename(json_file_path))[0] + ".sql"
    sql_file_path = os.path.join(srcl_vw_folder, sql_file_name)

    # Read the JSON file to get schemaName and tableName
    with open(json_file_path, "r") as json_file:
        json_data = json.load(json_file)
        schema_name = json_data.get("schemaName", "srcl")  # Default to "srcl" if schemaName is missing
        table_name = json_data.get("tableName", "unknown_table")  # Default to "unknown_table" if tableName is missing

    # Initialize the SELECT statement
    select_columns = []

    # Process each column
    for column in columns:
        # Check if the column is PII or SPII and the table is classified as "Confidential"
        if table_classification == "Confidential" and (is_pii == "Y" or is_spii == "Y"):
            if column == pii_column_name or column == spii_column_name:
                # Add the CASE statement for PII/SPII columns
                case_statement = (
                    f"CASE \n"
                    f"    WHEN user_is_member_of('current_user'()::name, 'pii_users_group'::name) \n"
                    f"    THEN cast({column} as VARCHAR) \n"
                    f"    ELSE sha2(cast({column} as VARCHAR), 256) \n"
                    f"END as {column}"
                )
                select_columns.append(case_statement)
            else:
                # Add the column as is
                select_columns.append(column)
        else:
            # Add the column as is
            select_columns.append(column)

    # Join the columns with each on a new line
    select_columns_formatted = ",\n    ".join(select_columns)

    # Create the final SELECT statement
    select_statement = f"SELECT \n    {select_columns_formatted}\nFROM {schema_name}.{table_name}"

    # Write the SELECT statement to the SQL file
    with open(sql_file_path, "w") as sql_file:
        sql_file.write(select_statement)

    print(f"SQL file '{sql_file_name}' created successfully in '{srcl_vw_folder}'!")