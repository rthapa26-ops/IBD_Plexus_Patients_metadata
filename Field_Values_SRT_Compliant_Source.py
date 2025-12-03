import pandas as pd  # For reading/writing CSV and manipulating tabular data
import os           # For checking if input files exist

# --- Configuration ---
INPUT_CSV_PATH = "Field_Values_SRT_Compliant.csv"  # Path to the input SRT-compliant Field Values CSV
OUTPUT_CSV_PATH = "Field_Values_SRT_Compliant_Source.csv"  # Path for the updated CSV with source column
SOURCE_COLUMN_NAME = "source"  # Name of the new column to indicate data source
SOURCE_VALUE = "IBD_Plexus"   # Static value to populate in the new 'source' column

def add_source_column(input_path: str, output_path: str, column_name: str, source_value: str):
    """
    Function to add a static source column to the Field Values table.
    Steps:
    1. Load the SRT-compliant CSV.
    2. Add a new column with a fixed value (source).
    3. Reorder columns to include the new column.
    4. Save the updated CSV.
    """
    
    # --- 1️⃣ Check if input file exists ---
    if not os.path.exists(input_path):
        print(f"Error: Input file not found at path: {input_path}. Please provide the file.")
        return  # Stop execution if file does not exist

    print(f"Loading data from: {input_path}...")
    
    # --- 2️⃣ Load CSV into a Pandas DataFrame ---
    try:
        df = pd.read_csv(
            input_path, 
            dtype={'PatientID': str},  # Ensure PatientID is treated as string
            keep_default_na=False      # Prevent Pandas from automatically converting blanks to NaN
        )
    except Exception as e:
        print(f"Error reading Field Values CSV: {e}")
        return  # Stop if file reading fails

    # --- 3️⃣ Add the new 'source' column ---
    print(f"Adding '{column_name}' column with value: **{source_value}**...")
    df[column_name] = source_value  # Add the column with the same static value for all rows

    # --- 4️⃣ Reorder columns for consistency ---
    desired_cols = ['PatientID', 'FieldName', 'FieldValue', column_name]  # Define column order
    df = df[desired_cols]  # Reorder DataFrame columns

    # --- 5️⃣ Save the updated CSV ---
    print(f"Saving updated DataFrame to: {output_path}...")
    try:
        df.to_csv(output_path, index=False)  # Save without the index column
        print("Save complete.")
        
        # --- 6️⃣ Preview and confirmation ---
        print("\n--- Updated Field Values Head (Preview) ---")
        print(df.head())  # Show first few rows
        print(f"\nTotal records saved: {len(df)}")  # Show total row count
        
    except Exception as e:
        print(f"An error occurred during saving: {e}")  # Catch any file writing errors

# -----------------------------------------------------------
# --- EXECUTION ---
# -----------------------------------------------------------

# Call the function to add 'source' column
add_source_column(INPUT_CSV_PATH, OUTPUT_CSV_PATH, SOURCE_COLUMN_NAME, SOURCE_VALUE)

