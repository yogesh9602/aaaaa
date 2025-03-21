Excel to YAML Converter
This project is designed to automate the process of converting data from an Excel file into YAML configuration files. It also generates SQL files based on JSON metadata and updates the schema name in the JSON files.

Project Structure

Excel to YAML Converter/
├── dex_ingestion/                  # Main Python package
│   ├── __init__.py                 # Makes the folder a Python package
│   ├── constants.py                # Contains constants like KEYWORDS_TO_RENAME
│   ├── excel_processor.py          # Processes the Excel file
│   ├── main.py                     # Main script to run the project
│   ├── sql_generator.py            # Generates SQL files
│   ├── utils.py                    # Utility functions (e.g., rename_column_if_keyword)
│   └── yaml_generator.py           # Generates YAML files
├── requirements.txt                # Lists all dependencies
├── secret_name.json                # Contains secret names for source databases
├── srcl/                           # Folder for input JSON files
│   └── ebt_rm_data.json            # Example JSON file
├── srcl_vw/                        # Folder for generated SQL files (created automatically)
├── temp/                           # Folder for updated JSON files (created automatically)
└── DEX-Table_Ingestion_Template-V1.xlsx  # Input Excel file

Features
1. Excel Processing:
Reads metadata from an Excel file (DEX-Table_Ingestion_Template-V1.xlsx).
Extracts table names, column names, and other metadata.

2. YAML Generation:
Generates YAML files (DBtoRedshift.yml and OGGToRedshift.yml) based on the processed data.

3. SQL Generation:
Generates .sql files for each table based on the JSON metadata.
Handles PII/SPII columns by adding CASE statements for secure data access.

4. JSON Updates:
Updates the schemaName in JSON files from "srcl" to "temp" and saves them in the temp folder.

5. Secret Name Integration:
Uses a secret_name.json file to populate the source_secret_name field in the YAML files.

Prerequisites
Python 3.7 or higher
Required Python packages (listed in requirements.txt)

Install Dependencies:
Install the required Python packages using:
pip install -r requirements.txt

Prepare Input Files:
Place your Excel file (DEX-Table_Ingestion_Template-V1.xlsx) in the root folder.
Add JSON files (e.g. ebt_rm_data.json) to the srcl folder.
Ensure the secret_name.json file is present in the root folder.

Run the Project:
Execute the main script using:
python -m dex_ingestion.main

Output
After running the project, the following outputs will be generated:

YAML Files:
DBtoRedshift.yml
OGGToRedshift.yml (if applicable)

SQL Files:
Generated .sql files will be saved in the srcl_vw folder.

Updated JSON Files:
Updated JSON files with schemaName changed to "temp" will be saved in the temp folder.

Troubleshooting
1. Missing Excel File:
Ensure the DEX-Table_Ingestion_Template-V1.xlsx file is present in the root folder.

2. Missing JSON Files:
Ensure the JSON files are present in the srcl folder.

3. Missing Secret File:
Ensure the secret_name.json file is present in the root folder.

4. Dependencies Not Installed:
Run pip install -r requirements.txt to install the required packages.
