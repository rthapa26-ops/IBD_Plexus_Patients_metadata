# SPARC Metadata Processing Workflow

## Overview

This workflow converts raw Excel metadata into **SRT-compliant tables** ready for ingestion into a database or the SRTAINTy pipeline.  

It uses multiple scripts that handle specific stages of the pipeline, including merging Excel sheets, normalizing data, generating database tables, creating SRT-compliant CSVs, and adding a source column.

---

## Scripts and Purpose

| Script | Description |
|--------|-------------|
| `SPARC_patients_metadata_merged_final.py` | Reads specified Excel sheets, prefixes column names with the sheet name, formats datetime columns, adds sequential record numbers, melts into long format, and outputs a single long-format CSV. |
| `SPARC_merged_final_all_files_SRTAINTYingestion.py` | Loads the long-format CSV and generates the three database tables: **Patients**, **Field Definitions** (with inferred types), and **Field Values**. Handles missing values accurately. |
| `SPARC_merged_final_all_files_SRTAINTYingestion_SRT_Compliant.py` | Cleans and sanitizes field names, maps data types to SRT standards, and outputs **SRT-compliant Field Values and Field Definitions CSVs**. |
| `Field_Values_SRT_Compliant_Source.py` | Adds a `source` column to the SRT-compliant Field Values CSV to track the origin of data. |

---

## Workflow Steps

```bash
# 1. Merge and Normalize Sheets
python SPARC_patients_metadata_merged_final.py
# Output:
# combined_long_format.csv

# 2. Generate Database Tables
python SPARC_merged_final_all_files_SRTAINTYingestion.py
# Outputs:
# Patient_Identifiers_Final.csv
# Field_Definitions_Final.csv
# Field_Values_Final.csv

# 3. Create SRT-Compliant Files
python SPARC_merged_final_all_files_SRTAINTYingestion_SRT_Compliant.py
# Outputs:
# Field_Values_SRT_Compliant.csv
# Field_Definitions_SRT_Compliant.csv

# 4. Add Source Column
python Field_Values_SRT_Compliant_Source.py
# Output:
# Field_Values_SRT_Compliant_Source.csv

