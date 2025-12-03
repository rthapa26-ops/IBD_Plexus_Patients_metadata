import pandas as pd
import os
import logging

# Assuming the ingestion module is saved as 'long_ingestion_script.py'
from long_ingestion_script import ingest_long_dataframe 
from srt.db.connection import DBEnvironment, DBRole, init_session, managed_session

logger = logging.getLogger(__name__)

# --- USER CONFIGURATION ---
# Define the actual file path and column names for the current dataset
DATA_FILE_PATH = "Field_Values_SRT_Compliant.csv" 
PATIENT_ID_COL = "PatientID" # e.g., 'Subject_ID', 'ID'
FIELD_NAME_COL = "FieldName"  # e.g., 'Feature_Name', 'Variable'
RAW_VALUE_COL = "FieldValue" # e.g., 'Value', 'Raw_Data'

# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting Long Format Data Ingestion Runner...")

    try:
        # 1. Load Data
        logger.info(f"Loading data from {DATA_FILE_PATH}...")
        long_df = pd.read_csv(DATA_FILE_PATH)

        # 2. Initialize Database Session
        # NOTE: DBRole.ADMIN and DBEnvironment.DEV/QA/PROD must be correctly configured
        logger.info("Initializing DEV database session...")
        session_maker = init_session(DBEnvironment.DEV, DBRole.ADMIN)

        # 3. Execute the Ingestion Pipeline
        with managed_session(session_maker) as session:
            ingest_long_dataframe(
                df=long_df, 
                session=session,
                patient_id_col=PATIENT_ID_COL,
                field_name_col=FIELD_NAME_COL,
                raw_value_col=RAW_VALUE_COL
            )
            
        logger.info("Ingestion process finished.")

    except FileNotFoundError:
        logger.error(f"Error: Data file not found at {DATA_FILE_PATH}")
    except ImportError as e:
        logger.error(f"Critical Import Error: Ensure 'long_ingestion_script.py' and 'srt.db.connection' are accessible. Details: {e}")
    except Exception as e:
        logger.critical(f"CRITICAL FAILURE during ingestion setup or execution: {e}")
