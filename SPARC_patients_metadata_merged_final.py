import pandas as pd
from typing import List, Dict
import os
import time

def process_excel_to_single_long_dataframe(file_path: str, sheets_to_process: List[str]) -> pd.DataFrame:
    """
    Reads specified sheets, prefixes all column names with the sheet name,
    processes the data into a normalized long format including a sequential
    record number, formats all datetime columns BEFORE melting, and concatenates
    everything into a single long-format DataFrame.
    """

    print("Starting individual sheet processing...")
    start_time = time.time()
    
    # -----------------------------
    # Check if the Excel file exists
    # -----------------------------
    if not os.path.exists(file_path):
        print(f"Error: File not found at path: {file_path}")
        return pd.DataFrame()

    try:
        # -----------------------------------------------
        # Read multiple sheets at once.
        # parse_dates=True → Pandas will try to detect any date columns.
        # dtype → ensure patient ID stays as a string (important for melting).
        # -----------------------------------------------
        all_data: Dict[str, pd.DataFrame] = pd.read_excel(
            file_path,
            sheet_name=sheets_to_process,
            parse_dates=True,
            dtype={'DEIDENTIFIED_MASTER_PATIENT_ID': str}
        )
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return pd.DataFrame()

    all_long_dfs: List[pd.DataFrame] = []
    key_column = 'DEIDENTIFIED_MASTER_PATIENT_ID'
    
    # ======================================================
    # Loop through each sheet in the Excel file
    # ======================================================
    for sheet_name, df in all_data.items():
        print(f"\nProcessing sheet: **{sheet_name}**")
            
        # ----------------------------------------------------
        # Ensure the key patient ID column exists
        # ----------------------------------------------------
        if key_column not in df.columns:
            print(f"Skipping sheet {sheet_name}: Missing '{key_column}'.")
            continue
            
        df = df.copy() 
        
        # ----------------------------------------------------
        # Standardize the column names:
        # - strip whitespace
        # - replace spaces with underscores
        # - uppercase everything
        # ----------------------------------------------------
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.upper()
        
        # ----------------------------------------------------
        # Prefix all non-ID columns with the sheet name 
        # e.g., column "AGE" in sheet "SUMMARY_ENROLLMENT"
        # becomes "SUMMARY_ENROLLMENT_AGE"
        # ----------------------------------------------------
        prefix = sheet_name.upper().replace(' ', '_') + '_'
        
        new_columns = {}
        for col in df.columns:
            if col != key_column:
                new_columns[col] = prefix + col
        
        df.rename(columns=new_columns, inplace=True)
            
        # ----------------------------------------------------
        # Remove fully duplicate rows (optional but useful)
        # ----------------------------------------------------
        initial_rows = len(df)
        df = df.drop_duplicates(subset=df.columns, keep='first')
        rows_removed = initial_rows - len(df)
        print(f"   - Removed {rows_removed} completely duplicate rows.")

        # ----------------------------------------------------
        # Add RECORD_NUMBER = 1,2,3... per patient ID
        # cumcount() gives 0,1,2... so add +1
        # ----------------------------------------------------
        df.loc[:, 'RECORD_NUMBER'] = df.groupby(key_column).cumcount() + 1
            
        # ----------------------------------------------------
        # Identify date columns detected by Pandas
        # ----------------------------------------------------
        date_cols = df.select_dtypes(include=['datetime64', 'datetime64[ns]']).columns
        
        if not date_cols.empty:
            print(f"   - Formatting {len(date_cols)} date columns to YYYY-MM-DD string format.")
            # Convert datetime columns to formatted strings BEFORE melting
            for col in date_cols:
                df[col] = df[col].dt.strftime('%Y-%m-%d').astype(str) 

        # ----------------------------------------------------
        # Prepare for melting (long format)
        # Exclude the patient ID and record number
        # ----------------------------------------------------
        value_vars = [col for col in df.columns if col not in [key_column, 'RECORD_NUMBER']]

        # ----------------------------------------------------
        # Melt the sheet into three columns:
        # PatientID | RECORD_NUMBER | Variable | Value
        # ----------------------------------------------------
        long_df = df.melt(
            id_vars=[key_column, 'RECORD_NUMBER'],
            value_vars=value_vars,
            var_name='Original_Variable',
            value_name='Value'
        )
                
        # ----------------------------------------------------
        # Construct final variable name:
        #   SHEETNAME_COLUMNNAME_RECORDNUMBER
        # Example:
        #   SUMMARY_ENROLLMENT_AGE_1
        # ----------------------------------------------------
        long_df['Final_Variable'] = (
            long_df['Original_Variable'] + '_' +
            long_df['RECORD_NUMBER'].astype(str)
        )
            
        # ----------------------------------------------------
        # Drop intermediate columns and rename for final output
        # ----------------------------------------------------
        final_long_df = long_df.drop(columns=['Original_Variable', 'RECORD_NUMBER'])
        final_long_df.rename(columns={'Final_Variable': 'Variable'}, inplace=True)
            
        print(f"   - Resulting normalized long format shape for {sheet_name}: {final_long_df.shape}")
            
        all_long_dfs.append(final_long_df)

    # ======================================================
    # Concatenate all transformed sheets together
    # ======================================================
    if not all_long_dfs:
        print("\n No dataframes were successfully processed.")
        return pd.DataFrame()
        
    final_combined_df = pd.concat(all_long_dfs, ignore_index=True)
    
    end_time = time.time()
    print("\n All sheets processed and concatenated into a single DataFrame.")
    print(f"Total processing time: {end_time - start_time:.2f} seconds.")
    print(f"Final Combined DataFrame shape: {final_combined_df.shape}")
    
    return final_combined_df


# =====================================================================
# Execution Block
# =====================================================================

file_path = "/home/ec2-user/2025-09-19_Full Cohort September/2025-09-19_Full Cohort September/2025-09-19_SPARC_Full Cohort September_REPORTS.xlsx"
output_csv_path = "/home/ec2-user/2025-09-19_Full Cohort September/2025-09-19_Full Cohort September/SPARC_Full_Cohort_Long_Format_COMBINED_CLEANED_VARS.csv"

sheets_to_process = [
    'SUMMARY_ENROLLMENT', 'SUMMARY_OMICS', 'SUMMARY_ENDOSCOPY', 'MEDICATION_OMICS', 'MEDICATION_ENROLLMENT', 'MEDICATION_ENDOSCOPY', 'MED_JOURNEY'
]

# Process the Excel file
final_combined_df = process_excel_to_single_long_dataframe(file_path, sheets_to_process)

# Save final combined CSV
if not final_combined_df.empty:
    try:
        print(f"\n Saving final combined DataFrame to CSV: {output_csv_path}")
        final_combined_df.to_csv(output_csv_path, index=False)
        print("Save complete.")
        
        print("\n--- Final Combined DataFrame Head (Preview) ---")
        print(final_combined_df.head(10))
        print(f"\nTotal rows saved to CSV: {len(final_combined_df)}")
        
    except Exception as e:
        print(f"\n An error occurred during saving: {e}")

