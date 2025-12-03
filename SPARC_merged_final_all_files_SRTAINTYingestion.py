import pandas as pd  # Import Pandas for data manipulation
import os            # Import os for file path checking

# --- Configuration ---
# Path to  already created combined long-format CSV file
INPUT_CSV_PATH = "/home/ec2-user/2025-09-19_Full Cohort September/2025-09-19_Full Cohort September/SPARC_Full_Cohort_Long_Format_COMBINED_CLEANED_VARS.csv"
KEY_COLUMN = 'DEIDENTIFIED_MASTER_PATIENT_ID'  # Column identifying unique patients

# --- Output Files ---
OUTPUT_PATIENTS_CSV = "Patient_Identifiers_Final.csv"      # CSV for Patients table
OUTPUT_FIELD_DEFS_CSV = "Field_Definitions_Final.csv"     # CSV for Field Definitions table
OUTPUT_FIELD_VALUES_CSV = "Field_Values_Final.csv"        # CSV for Field Values table (renamed input)

# --- Function Definition ---
def generate_database_tables(input_path: str):
    """
    Loads the long-format CSV and extracts data to populate:
    1. Patients table
    2. Field Definitions table (with inferred types)
    3. Field Values table
    Handles missing values accurately when inferring types.
    """
    
    # Check if input file exists
    if not os.path.exists(input_path):
        print(f"Error: Input file not found at path: {input_path}")
        return

    print("Loading combined long-format data...")
    try:
        # Load CSV
        combined_df = pd.read_csv(
            input_path, 
            dtype={KEY_COLUMN: str},    # Ensure patient IDs are read as strings
            keep_default_na=False       # Keep empty strings as-is, don't convert to NaN
        )
    except Exception as e:
        print(f"Error reading combined CSV: {e}")
        return

    # --- 1. Extract Patients Table ---
    print("\nExtraction 1/3: Generating Patient Identifiers...")
    patient_ids_df = combined_df[[KEY_COLUMN]].drop_duplicates().reset_index(drop=True)  # Unique patients
    patient_ids_df.rename(columns={KEY_COLUMN: 'PatientID'}, inplace=True)              # Rename column
    patient_ids_df.to_csv(OUTPUT_PATIENTS_CSV, index=False)                             # Save to CSV
    print(f"Patient IDs saved to {OUTPUT_PATIENTS_CSV} ({len(patient_ids_df)} unique IDs).")

    # --- 2. Extract Field Definitions Table ---
    print("\nExtraction 2/3: Generating Field Definitions and Inferring Types...")
    
    # a. Get all unique variable names
    field_definitions_df = combined_df[['Variable']].drop_duplicates().reset_index(drop=True)

    # b. Function to infer data type, considering missing values
    def infer_data_type(series):
        """
        Infers the type of a variable.
        Returns: 'integer', 'float', 'date', or 'string'
        Ignores NA/missing values when computing percentages.
        """
        series = series.dropna()  # Remove NA values before type inference
        
        if len(series) == 0:     # If all values are NA, treat as string
            return 'string'
        
        # Attempt numeric conversion
        numeric_series = pd.to_numeric(series, errors='coerce')  # Coerce non-numeric to NaN
        numeric_count = numeric_series.notna().sum()             # Count of numeric values
        
        # If >=80% of non-NA values are numeric
        if numeric_count / len(series) > 0.8:
            # If >=95% of numeric values are integers
            if ((numeric_series % 1 == 0).sum() / numeric_count) > 0.95:
                return 'integer'
            return 'float'  # Otherwise, float
        
        # Check if all remaining values look like dates (YYYY-MM-DD)
        if series.str.match(r'^\d{4}-\d{2}-\d{2}$').all():
            return 'date'
        
        # Default fallback
        return 'string'

    # c. Apply type inference for each variable
    type_df = combined_df.groupby('Variable')['Value'].apply(
        lambda x: infer_data_type(x.astype(str))  # Convert values to string before inference
    ).reset_index(name='Data_Type')

    # d. Merge inferred types into field definitions
    field_definitions_df = field_definitions_df.merge(type_df, on='Variable', how='left')
    field_definitions_df.rename(columns={'Variable': 'FieldName'}, inplace=True)      # Rename column
    field_definitions_df['Description'] = field_definitions_df['FieldName']          # Placeholder description
    field_definitions_df.to_csv(OUTPUT_FIELD_DEFS_CSV, index=False)                  # Save to CSV
    print(f"Field Definitions saved to {OUTPUT_FIELD_DEFS_CSV} ({len(field_definitions_df)} fields).")

    # --- 3. Prepare Field Values Table ---
    print("\nExtraction 3/3: Preparing Field Values table...")
    field_values_df = combined_df.rename(columns={
        KEY_COLUMN: 'PatientID',  # Rename patient ID column
        'Variable': 'FieldName',  # Rename variable column
        'Value': 'FieldValue'     # Rename value column
    })
    field_values_df.to_csv(OUTPUT_FIELD_VALUES_CSV, index=False)  # Save to CSV
    print(f"Field Values saved to {OUTPUT_FIELD_VALUES_CSV} ({len(field_values_df)} records).")

# --- Execute ---
generate_database_tables(INPUT_CSV_PATH)

