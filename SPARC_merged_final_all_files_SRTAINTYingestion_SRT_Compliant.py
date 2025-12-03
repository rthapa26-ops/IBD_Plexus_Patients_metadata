import pandas as pd       # For data manipulation and CSV handling
import numpy as np        # For handling numerical operations and NaN
import os                 # For checking file existence
import re                 # For regular expressions (used to sanitize field names)

# --- Configuration ---
INPUT_CSV_PATH = "Field_Values_Final.csv"  # Input CSV containing patient field values
OUTPUT_FIELD_VALUES_COMPLIANT_CSV = "Field_Values_SRT_Compliant.csv"  # Output compliant Field Values table
OUTPUT_FIELD_DEFS_COMPLIANT_CSV = "Field_Definitions_SRT_Compliant.csv"  # Output compliant Field Definitions table
OUTPUT_PATIENTS_CSV = "Patient_Identifiers_Final.csv"  # Output patient identifiers table

# Mapping inferred data types to official SRT database types
SRT_FIELD_TYPES = {
    'string': 'string',   # String remains string
    'date': 'date',       # Date remains date
    'float': 'float',     # Float remains float
    'integer': 'int',     # Integer maps to int in SRT
    'boolean': 'boolean'  # Boolean remains boolean
}

def sanitize_field_name(name: str) -> str:
    """
    Sanitizes field names for database compliance.
    - Converts to lowercase
    - Replaces any non-alphanumeric characters (except underscores) with underscores
    - Strips leading and trailing underscores
    Example: 'Blood Pressure(mmHg)' -> 'blood_pressure_mmhg'
    """
    sanitized = re.sub(r'[^a-z0-9_]+', '_', name.lower())  # Replace invalid chars with '_'
    return sanitized.strip('_')  # Remove leading/trailing underscores

def create_srt_compliant_files(input_path: str):
    """
    Main function to create SRT-compliant tables:
    1. Extract unique patient identifiers
    2. Filter and sanitize Field Values
    3. Sanitize Field Definitions and map data types
    """
    
    # Check if the input file exists
    if not os.path.exists(input_path):
        print(f"Error: Input file not found at path: {input_path}")
        return

    print("Loading Field Values data for compliance checks...")
    
    try:
        # Load Field Values CSV
        df = pd.read_csv(
            input_path, 
            dtype={'PatientID': str},  # Ensure PatientID is treated as string
            na_values=['NA', 'N/A', '', ' '],  # Treat these as NaN
            keep_default_na=True  # Include default NaN values
        )
    except Exception as e:
        print(f"Error reading Field Values CSV: {e}")
        return

    # --- 1️⃣ Extract Patient Identifiers ---
    print("\nExtraction 1/3: Saving Patient Identifiers (Unblocking Step)...")
    patient_ids_df = df[['PatientID']].drop_duplicates().reset_index(drop=True)  # Remove duplicates
    patient_ids_df.to_csv(OUTPUT_PATIENTS_CSV, index=False)  # Save CSV
    print(f"Patient IDs saved to {OUTPUT_PATIENTS_CSV} ({len(patient_ids_df)} unique IDs).")

    # --- 2️⃣ Filter and Sanitize Field Values ---
    print("\nCleaning Field Values: Filtering out implicit/explicit nulls...")
    initial_count = len(df)  # Save original number of rows

    # Drop rows where FieldValue is NaN (explicit or implicit nulls)
    df_filtered = df.dropna(subset=['FieldValue'])
    rows_removed = initial_count - len(df_filtered)  # Count how many rows removed
    print(f"   - Removed {rows_removed} null/NA rows. New size: {len(df_filtered)}")

    # Sanitize FieldName for database compliance
    df_filtered.loc[:, 'FieldName_Sanitized'] = df_filtered['FieldName'].apply(sanitize_field_name)

    # Prepare final Field Values table for SRT
    field_values_final = df_filtered[['PatientID', 'FieldName_Sanitized', 'FieldValue']].copy()
    field_values_final.rename(columns={'FieldName_Sanitized': 'FieldName'}, inplace=True)  # Rename sanitized column
    field_values_final.to_csv(OUTPUT_FIELD_VALUES_COMPLIANT_CSV, index=False)  # Save CSV
    print(f"Field Values (Compliant) saved to {OUTPUT_FIELD_VALUES_COMPLIANT_CSV}.")

    # --- 3️⃣ Create Compliant Field Definitions ---
    print("\nCreating Compliant Field Definitions (Sanitized Names & Mapped Types)...")

    # Load original Field Definitions CSV
    if not os.path.exists("Field_Definitions_Final.csv"):
        print("Cannot find 'Field_Definitions_Final.csv'. Skipping Field Definitions creation.")
        return

    defs_df = pd.read_csv("Field_Definitions_Final.csv")

    # Sanitize FieldName column
    defs_df['FieldName_Sanitized'] = defs_df['FieldName'].apply(sanitize_field_name)

    # Map original Data_Type to SRT-compliant types
    defs_df['Data_Type_Mapped'] = defs_df['Data_Type'].map(SRT_FIELD_TYPES)

    # If mapping failed (type not recognized), default to string
    defs_df.loc[defs_df['Data_Type_Mapped'].isna(), 'Data_Type_Mapped'] = 'string'

    # Finalize Field Definitions table for SRT
    field_defs_final = defs_df[['FieldName_Sanitized', 'Data_Type_Mapped', 'Description']].copy()
    field_defs_final.rename(
        columns={'FieldName_Sanitized': 'FieldName', 'Data_Type_Mapped': 'DataType'},
        inplace=True
    )
    field_defs_final.to_csv(OUTPUT_FIELD_DEFS_COMPLIANT_CSV, index=False)  # Save CSV
    print(f"Field Definitions (Compliant) saved to {OUTPUT_FIELD_DEFS_COMPLIANT_CSV}.")

# --- EXECUTION ---
create_srt_compliant_files(INPUT_CSV_PATH)  # Run the function

