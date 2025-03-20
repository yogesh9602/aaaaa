# main.py

from .excel_processor import process_excel
from .yaml_generator import generate_yaml

def main():
    input_excel = "DEX-Table_Ingestion_Template-V1.xlsx"  # Updated input file
    output_yaml = "DBtoRedshift.yml"  # DBtoRedshift tasks
    output_yaml2 = "OGGToRedshift.yml"  # OGGToRedshift tasks
    json_folder = "srcl"  # Folder containing JSON files

    # Process the Excel file
    df = process_excel(input_excel, json_folder)

    # Generate YAML files
    generate_yaml(df, output_yaml, output_yaml2, json_folder)

if __name__ == "__main__":
    main()