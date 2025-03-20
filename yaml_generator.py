# yaml_generator.py

import pandas as pd
import yaml
import json
import os
import shutil
from .utils import rename_column_if_keyword
from .sql_generator import generate_sql_file  # Import the SQL generator function

# Define the additional columns to add if the DB type is not 'sybase'
ADDITIONAL_COLUMNS = [
    {
        "name": "ETL_CREATED_BY",
        "type": "varchar(255)",
        "encoding": "ZSTD",
        "isActive": True,
        "nullable": True,
        "default": None
    },
    {
        "name": "ETL_CREATED_TIME",
        "type": "timestamp",
        "encoding": "ZSTD",
        "isActive": True,
        "nullable": True,
        "default": None
    },
    {
        "name": "ETL_LAST_UPDATED_BY",
        "type": "varchar(255)",
        "encoding": "ZSTD",
        "isActive": True,
        "nullable": True,
        "default": None
    },
    {
        "name": "ETL_LAST_UPDATED_TIME",
        "type": "timestamp",
        "encoding": "ZSTD",
        "isActive": True,
        "nullable": True,
        "default": None
    },
    {
        "name": "ETL_CHANGE_FLAG",
        "type": "varchar(1)",
        "encoding": "ZSTD",
        "isActive": True,
        "nullable": True,
        "default": None
    },
    {
        "name": "WF_AUDIT_ID",
        "type": "integer",
        "encoding": "ZSTD",
        "isActive": True,
        "nullable": True,
        "default": None
    }
]

# Define the OGG-related columns to add if the DB type is not 'sybase' and task2 is 'OGGToRedshift'
OGG_COLUMNS = [
    {
        "name": "OGG_POS",
        "type": "BIGINT",
        "encoding": "RAW",
        "isActive": True,
        "nullable": True,
        "default": None
    },
    {
        "name": "OGG_SEQUENCE_NUMBER",
        "type": "BIGINT",
        "encoding": "RAW",
        "isActive": True,
        "nullable": True,
        "default": None
    },
    {
        "name": "OGG_COPY_TIMESTAMP",
        "type": "TIMESTAMP",
        "encoding": "RAW",
        "isActive": True,
        "nullable": True,
        "default": None
    },
    {
        "name": "OGG_COMMIT_TIMESTAMP",
        "type": "TIMESTAMP",
        "encoding": "RAW",
        "isActive": True,
        "nullable": True,
        "default": None
    }
]

# List of OGG columns to exclude from DBtoRedshift's column_list
OGG_COLUMNS_TO_EXCLUDE = {
    "OGG_POS",
    "OGG_SEQUENCE_NUMBER",
    "OGG_COPY_TIMESTAMP",
    "OGG_COMMIT_TIMESTAMP"
}

def load_secret_names(secret_file_path):
    """
    Load the secret names from the secret_name.json file.
    """
    if os.path.exists(secret_file_path):
        with open(secret_file_path, "r") as secret_file:
            return json.load(secret_file)
    else:
        print(f"Warning: Secret file '{secret_file_path}' not found.")
        return []

def get_secret_name(source_db, secret_names):
    """
    Get the secret name for the given source_db from the secret_names list.
    """
    for secret in secret_names:
        if secret["source_db"] == source_db:
            return secret["secret_name"]
    return None

def create_temp_folder_and_update_schema(json_folder):
    """
    Create a temp folder and copy all JSON files from the srcl folder to it.
    Update the schemaName from "srcl" to "temp" in each JSON file.
    """
    # Define the path for the temp folder
    temp_folder = os.path.join(os.path.dirname(json_folder), "temp")

    # Create the temp folder if it doesn't exist
    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)
        print(f"Created temp folder at: {temp_folder}")

    # Iterate over all JSON files in the srcl folder
    for json_file_name in os.listdir(json_folder):
        if json_file_name.endswith(".json"):
            # Define the source and destination paths
            src_path = os.path.join(json_folder, json_file_name)
            dest_path = os.path.join(temp_folder, json_file_name)

            # Read the JSON file
            with open(src_path, "r") as json_file:
                json_data = json.load(json_file)

            # Update the schemaName from "srcl" to "temp"
            if "schemaName" in json_data and json_data["schemaName"] == "srcl":
                json_data["schemaName"] = "temp"

            # Write the updated JSON data to the temp folder
            with open(dest_path, "w") as json_file:
                json.dump(json_data, json_file, indent=4)

            print(f"Updated schemaName in '{json_file_name}' and copied to temp folder.")

def generate_yaml(df, output_yaml, output_yaml2, json_folder):
    # Initialize empty lists to store the YAML data
    yaml_data = []
    yaml_data2 = []

    # Load the secret names from the secret_name.json file (located outside the srcl folder)
    secret_file_path = os.path.join(os.path.dirname(json_folder), "secret_name.json")
    secret_names = load_secret_names(secret_file_path)

    # Create the srcl_vw folder if it doesn't exist
    srcl_vw_folder = os.path.join(os.path.dirname(json_folder), "srcl_vw")
    if not os.path.exists(srcl_vw_folder):
        os.makedirs(srcl_vw_folder)
        print(f"Created srcl_vw folder at: {srcl_vw_folder}")

    # Process each row in the DataFrame
    for index, row in df.iterrows():
        # Extract the table name
        table_name = row["source table name"]

        # Debug: Print table_name and its type
        print(f"Processing row {index + 1}: table_name = {table_name}, type = {type(table_name)}")

        # Skip rows with missing table_name
        if pd.isna(table_name):
            print(f"Skipping row {index + 1} because 'source table name' is missing.")
            continue

        # Split the table name to get schema and table
        try:
            schema_name, table_name_without_schema = table_name.split(".", 1)
        except AttributeError:
            print(f"Error: 'source table name' is not a valid string in row {index + 1}. Value: {table_name}")
            continue

        # Create the target_table by prepending the system name and replacing '.' with '_'
        target_table = f"{row['source system name']}_{table_name_without_schema}"

        # Get the Data Refresh Frequency value
        refresh_frequency = row["data refresh frequency"]
        if pd.isna(refresh_frequency):
            refresh_frequency = "unknown"  # Default value if missing

        # Create the task_id by prepending 'de_etl_' to the target_table and appending refresh_frequency
        task_id = f"de_etl_{target_table}_{refresh_frequency.lower()}"

        # Read the JSON file to get column names
        json_file_name = row["json file"]
        json_file_path = os.path.join(json_folder, json_file_name)

        # Initialize an empty list for column names
        column_list = []
        column_rename = {}  # Dictionary to track renamed columns

        # Extract the table classification, PII, and SPII information
        table_classification = row["table classification"]
        is_pii = row["is pii [y/n]"]
        is_spii = row["is spii [y/n]"]
        pii_column_name = row["pii column name"]
        spii_column_name = row["spii column name"]

        # Get the source_db name from the row
        source_db = row["source system database name"]

        # Get the secret name for the source_db
        secret_name = get_secret_name(source_db, secret_names)

        # Check if the JSON file exists
        if os.path.exists(json_file_path):
            with open(json_file_path, "r") as json_file:
                json_data = json.load(json_file)
                # Extract column names from the "columns" array
                for column in json_data.get("columns", []):
                    column_name = column.get("name")
                    if column_name:
                        # Rename the column if it matches any keyword
                        new_column_name = rename_column_if_keyword(column_name)
                        if new_column_name != column_name:
                            column_rename[column_name] = new_column_name  # Track renamed columns
                        column_list.append(new_column_name)
                        # Update the column name in the JSON data
                        column["name"] = new_column_name

                # Add additional columns if the DB type is not 'sybase'
                if row["source system db type"].lower() != "sybase":
                    json_data["columns"].extend(ADDITIONAL_COLUMNS)
                    # Add the additional column names to the column_list
                    for col in ADDITIONAL_COLUMNS:
                        column_list.append(col["name"])

                # Add OGG-related columns if the DB type is not 'sybase' and task2 is 'OGGToRedshift'
                if row["source system db type"].lower() != "sybase" and row["task2"] == "OGGToRedshift":
                    json_data["columns"].extend(OGG_COLUMNS)
                    # Add the OGG column names to the column_list
                    for col in OGG_COLUMNS:
                        column_list.append(col["name"])

            # Write the updated JSON data back to the file
            with open(json_file_path, "w") as json_file:
                json.dump(json_data, json_file, indent=4)

            # Generate the SQL file for this JSON file
            generate_sql_file(
                json_file_path,  # Pass the JSON file path
                column_list,
                table_classification,
                is_pii,
                is_spii,
                pii_column_name,
                spii_column_name,
                srcl_vw_folder  # Pass the srcl_vw folder path
            )
        else:
            print(f"Warning: JSON file '{json_file_name}' not found for table '{table_name}'.")

        # Debug: Print column_rename dictionary
        print(f"Column rename dictionary for row {index + 1}: {column_rename}")

        # Create the nested structure for DBtoRedshift
        db_to_redshift = {
            "DBtoRedshift": {
                "task_id": task_id,
                "source_db": source_db,
                "source_schema": schema_name,
                "source_table": table_name_without_schema,
                "source_secret_name": secret_name,  # Use the secret name if available, otherwise null
                "source_type": row["source system db type"],
                "target_schema": "srcl",  # Fixed value for target_schema
                "target_database": "kmbl_dex",
                "target_table": target_table,
                "db_user": "de_etl_role",
                "transformation_function": "bods_truncate_and_load_transformation",
                "column_list": [col for col in column_list if col not in OGG_COLUMNS_TO_EXCLUDE]  # Exclude OGG columns
            }
        }

        # Add column_rename parameter if any columns were renamed
        if column_rename:
            db_to_redshift["DBtoRedshift"]["column_rename"] = column_rename

        # Check if "Source Table Size (GB)" is greater than 10
        source_table_size_gb = row["source table size (gb)"]
        if pd.notna(source_table_size_gb) and source_table_size_gb > 10:
            # Add source_partition_column and source_predicate_count after target_table
            reliable_date_column = row["reliable date column"]
            if pd.notna(reliable_date_column):
                db_to_redshift["DBtoRedshift"]["source_partition_column"] = reliable_date_column
                db_to_redshift["DBtoRedshift"]["source_predicate_count"] = 20  # Fixed value

        # Append this structure to the YAML data list
        yaml_data.append(db_to_redshift)

        # Check if the table is archived (Table Archived (Y/N) == 'Y')
        if row["table archived (y/n)"] == "Y":
            # Create a new task for the historical/archived table
            hist_task_id = f"{task_id}_hist"  # Append '_hist' to the task_id
            hist_source_schema = row["source archival/history schema name"]
            hist_source_table = row["source archival/history table name"]

            # Extract the table name without the schema and without appending '_hist'
            hist_source_table_without_schema = hist_source_table.split(".")[-1]

            # Create the nested structure for the historical task
            hist_db_to_redshift = {
                "DBtoRedshift": {
                    "task_id": hist_task_id,
                    "source_db": source_db,
                    "source_schema": hist_source_schema,  # Use value from "Source Archival/History Schema Name"
                    "source_table": hist_source_table_without_schema,  # Use value from "Source Archival/History Table Name" without schema
                    "source_secret_name": secret_name,  # Use the secret name if available, otherwise null
                    "source_type": row["source system db type"],
                    "target_schema": "srcl",  # Fixed value for target_schema
                    "target_database": "kmbl_dex",
                    "target_table": target_table,
                    "db_user": "de_etl_role",
                    "transformation_function": "bods_truncate_and_load_transformation",
                    "column_list": [col for col in column_list if col not in OGG_COLUMNS_TO_EXCLUDE]  # Exclude OGG columns
                }
            }

            # Add column_rename parameter if any columns were renamed
            if column_rename:
                hist_db_to_redshift["DBtoRedshift"]["column_rename"] = column_rename

            # Check if "Source Table Size (GB)" is greater than 10 for historical task
            if pd.notna(source_table_size_gb) and source_table_size_gb > 10:
                # Add source_partition_column and source_predicate_count after target_table
                reliable_date_column = row["reliable date column"]
                if pd.notna(reliable_date_column):
                    hist_db_to_redshift["DBtoRedshift"]["source_partition_column"] = reliable_date_column
                    hist_db_to_redshift["DBtoRedshift"]["source_predicate_count"] = 20  # Fixed value

            # Append this structure to the YAML data list
            yaml_data.append(hist_db_to_redshift)

        # Check if task2 is OGGToRedshift
        if row["task2"] == "OGGToRedshift":
            # Replace 'etl' with 'ogg' in task_id
            ogg_task_id = task_id.replace("de_etl_", "de_ogg_")

            # Handle primary_keys (single or multiple values)
            primary_keys = row["source table pk"]
            if pd.notna(primary_keys):
                primary_keys_list = [key.strip() for key in primary_keys.split(",")]
            else:
                primary_keys_list = None

            # Create the nested structure for OGGToRedshift
            ogg_to_redshift = {
                "OGGToRedshift": {
                    "task_id": ogg_task_id,
                    "source_db": source_db,
                    "source_schema": schema_name,
                    "primary_keys": primary_keys_list,
                    "redshift_table": target_table,
                    "db_user": "de_etl_role",
                    "column_list": column_list if column_list else None
                }
            }

            # Add column_rename parameter if any columns were renamed
            if column_rename:
                ogg_to_redshift["OGGToRedshift"]["column_rename"] = column_rename

            # Append this structure to the YAML data list for OGGToRedshift.yml
            yaml_data2.append(ogg_to_redshift)

    # Write to YAML file for DBtoRedshift
    with open(output_yaml, "w") as yaml_file:
        yaml.dump(yaml_data, yaml_file, default_flow_style=False, sort_keys=False, default_style=None)

    print(f"YAML file '{output_yaml}' created successfully!")

    # Write to YAML file for OGGToRedshift (if any data exists)
    if yaml_data2:
        with open(output_yaml2, "w") as yaml_file2:
            yaml.dump(yaml_data2, yaml_file2, default_flow_style=False, sort_keys=False, default_style=None)

        print(f"YAML file '{output_yaml2}' created successfully!")
    else:
        print("No OGGToRedshift tasks found. Skipping creation of OGGToRedshift.yml.")

    # Create the temp folder and update schemaName in JSON files after all changes
    create_temp_folder_and_update_schema(json_folder)