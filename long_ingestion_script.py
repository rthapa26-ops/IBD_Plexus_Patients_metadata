import logging
import pandas as pd
from sqlalchemy.orm import Session
from pandas._typing import DtypeObj

# --- SRT Imports ---
from srt.db.models import FieldType
from srt.services.factory import ServiceFactory
from srt.services.schemas.field_definitions import FieldDefinitionCreate
from srt.services.schemas.field_values import FieldValueCreate
from srt.services.schemas.patients import PatientCreate

# Note: parse_dataframe utilities are no longer needed/used in this long-format script
# from srt.services.utilities.parse_dataframe import (
#     cast_boolean_columns_to_bool,
#     cast_date_columns_to_date,
#     cast_valid_numeric_columns_to_int,
# )

logger = logging.getLogger(__name__)

# --- Reusable Configuration ---
DEFAULT_BATCH_SIZE = 5000

# ----------------------------------------------------------------------
# --- REUSED SRT UTILITY FUNCTIONS ---
# ----------------------------------------------------------------------

def map_pandas_type_to_field_type(dtype: DtypeObj) -> FieldType:
    # Logic remains the same: maps Pandas inferred type to SRT FieldType
    if pd.api.types.is_bool_dtype(dtype):
        return FieldType.BOOLEAN
    elif pd.api.types.is_integer_dtype(dtype):
        return FieldType.INT
    elif pd.api.types.is_float_dtype(dtype):
        return FieldType.FLOAT
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return FieldType.DATE
    else:
        return FieldType.STRING

def get_unique_patients(df: pd.DataFrame, patient_id_col: str) -> list[PatientCreate]:
    """Extracts unique patient IDs from the specified column."""
    return [PatientCreate(id=x) for x in df[patient_id_col].unique()]

# ----------------------------------------------------------------------
# --- LONG-FORMAT SPECIFIC FUNCTIONS ---
# ----------------------------------------------------------------------

def get_definitions_from_long_data(
    df: pd.DataFrame, 
    field_name_col: str, 
    raw_value_col: str
) -> list[FieldDefinitionCreate]:
    """
    Infers data types for each unique FieldName in the long-format data, 
    using the values in the specified raw_value_col.
    """
    field_defs: list[FieldDefinitionCreate] = []
    
    unique_field_names = df[field_name_col].unique()
    
    for field_name in unique_field_names:
        # Get all raw values for this specific field
        values = df[df[field_name_col] == field_name][raw_value_col].dropna()
        
        if values.empty:
            field_type = FieldType.STRING
        else:
            # 1. Date Inference (90% threshold)
            inferred_series = pd.to_datetime(values, errors='coerce')
            
            if inferred_series.notna().sum() > 0.9 * len(values):
                field_type = FieldType.DATE
            else:
                # 2. Numeric Inference (90% threshold)
                inferred_series = pd.to_numeric(values, errors='coerce')
                if inferred_series.notna().sum() > 0.9 * len(values):
                    # It's numeric (int or float)
                    dtype = pd.api.types.infer_dtype(inferred_series.dropna(), skipna=True)
                    if dtype == 'integer':
                        field_type = FieldType.INT
                    elif dtype == 'floating':
                        field_type = FieldType.FLOAT
                    else:
                        field_type = FieldType.STRING
                else:
                    # 3. Default to String
                    field_type = FieldType.STRING

        # Note: is_delimited is set to False for simplicity unless determined otherwise
        field_defs.append(
            FieldDefinitionCreate(
                field_name=field_name,
                field_type=field_type,
                is_delimited=False, 
                delimiter=None,
            )
        )
        
    return field_defs

def get_field_values_from_long_data(
    df: pd.DataFrame, 
    patient_id_col: str, 
    field_name_col: str, 
    raw_value_col: str
) -> list[FieldValueCreate]:
    """
    Directly maps the long-format DataFrame to FieldValueCreate objects row by row.
    """
    field_values: list[FieldValueCreate] = []
    
    # Iterate through the DataFrame row by row for direct mapping
    for _, row in df.iterrows():
        raw_value = str(row[raw_value_col])
        
        if pd.isna(row[raw_value_col]) or raw_value.strip() == "":
            continue # Skip null or empty values
            
        field_values.append(
            FieldValueCreate(
                patient_id=row[patient_id_col],
                field_name=row[field_name_col],
                raw_value=raw_value,
            )
        )
        
    return field_values

def ingest_long_dataframe(
    df: pd.DataFrame,
    session: Session,
    patient_id_col: str,
    field_name_col: str,
    raw_value_col: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    """Main ETL pipeline for long-format data."""
    try:
        factory = ServiceFactory(session)
        patient_service = factory.patient_service
        field_def_service = factory.field_definition_service
        field_value_service = factory.field_value_service
        
        # 1. Ingest Patients
        patients = get_unique_patients(df, patient_id_col)
        logger.info(f"Ingesting {len(patients)} unique patients.")
        patient_service.create_many(patients)
        
        # 2. Ingest Field Definitions (Metadata)
        field_defs = get_definitions_from_long_data(df, field_name_col, raw_value_col)
        logger.info(f"Ingesting {len(field_defs)} unique field definitions.")
        field_def_service.create_many(field_defs)

        # 3. Ingest Field Values in Batches
        logger.info("Starting Field Value ingestion in batches...")
        
        all_field_values = get_field_values_from_long_data(df, patient_id_col, field_name_col, raw_value_col)
        
        for start_idx in range(0, len(all_field_values), batch_size):
            batch = all_field_values[start_idx : start_idx + batch_size]
            field_value_service.create_many(batch)
            logger.info(f"Processed batch up to row {start_idx + len(batch)}.")

        session.commit()
        logger.info("\n🎉 Full Ingestion SUCCESSFUL.")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error during long-format ingestion: {e}")
        raise
