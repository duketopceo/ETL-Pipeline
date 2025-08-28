# ================================================================
# COMPREHENSIVE ETL DATA CLEANING PIPELINE LOGIC (V2)
# Date: 2025-08-28
# Author: Jules
# Purpose: Core logic for data cleaning, now with data quality flagging,
#          smarter data preservation, and reporting capabilities for Looker/BigQuery.
# ================================================================

import pandas as pd
import numpy as np
import logging
import re
from datetime import datetime
import warnings
import io

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_cleaning_log.txt', mode='w'),
        logging.StreamHandler()
    ],
    force=True
)
logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')

# ================================================================
# DATA ASSESSMENT (Unchanged)
# ================================================================
def assess_raw_data(file_content, file_name):
    """
    Assesses raw file content to determine properties like encoding and delimiter.
    This function remains the same as it correctly assesses file properties.
    """
    logger.info(f"ASSESSING: {file_name}")
    assessment = {'file_name': file_name, 'issues_found': [], 'working_encoding': 'utf-8', 'likely_delimiter': ','}
    decoded_content = ""
    try:
        decoded_content = file_content.decode('utf-8')
    except UnicodeDecodeError:
        for encoding in ['latin-1', 'cp1252', 'iso-8859-1']:
            try:
                decoded_content = file_content.decode(encoding)
                assessment['working_encoding'] = encoding
                break
            except UnicodeDecodeError:
                continue
    if not decoded_content:
        assessment['issues_found'].append("Failed to decode file.")
        return assessment

    lines = decoded_content.splitlines()
    comma_counts = [line.count(',') for line in lines[:20]]
    if comma_counts and max(comma_counts) > 0:
        assessment['likely_delimiter'] = ','

    try:
        df_sample = pd.read_csv(io.StringIO(decoded_content), delimiter=assessment['likely_delimiter'], nrows=100)
        assessment['initial_columns'] = list(df_sample.columns)
    except Exception as e:
        assessment['issues_found'].append(f"Pandas read error: {e}")

    return assessment

# ================================================================
# DATA QUALITY REPORTING (New Function)
# ================================================================
def generate_data_quality_report(df):
    """
    Generates a DataFrame containing a data quality report.

    Args:
        df (pd.DataFrame): The DataFrame to analyze.

    Returns:
        pd.DataFrame: A DataFrame with data quality statistics for each column.
    """
    logger.info("Generating data quality report...")
    quality_report = {
        'column_name': [],
        'data_type': [],
        'missing_count': [],
        'missing_percentage': [],
        'unique_values': []
    }
    for col in df.columns:
        quality_report['column_name'].append(col)
        quality_report['data_type'].append(df[col].dtype)
        missing_count = df[col].isnull().sum()
        quality_report['missing_count'].append(missing_count)
        missing_percentage = f"{(missing_count / len(df) * 100):.2f}%"
        quality_report['missing_percentage'].append(missing_percentage)
        quality_report['unique_values'].append(df[col].nunique())

    report_df = pd.DataFrame(quality_report)
    logger.info("✅ Data quality report generated.")
    return report_df

# ================================================================
# V2 DATA CLEANING AND TRANSFORMATION (Revised Logic)
# ================================================================
def clean_csv_data(file_content, file_name, assessment_results=None):
    """
    Cleans and transforms raw CSV data with revised, smarter logic.

    V2 Changes:
    - No longer drops columns with >90% missing data to preserve financial info.
    - Implements more conservative filling for missing values.
    - Adds a new 'is_complete' column to flag high-quality rows.
    """
    logger.info("=" * 60)
    logger.info(f"STARTING V2 DATA CLEANING FOR: {file_name}")
    logger.info("=" * 60)

    # --- Steps 1-4 are largely unchanged ---
    # 1. Load Data
    logger.info("STEP 1: Loading data...")
    try:
        delimiter = assessment_results.get('likely_delimiter', ',') if assessment_results else ','
        encoding = assessment_results.get('working_encoding', 'utf-8') if assessment_results else 'utf-8'
        df = pd.read_csv(io.BytesIO(file_content), delimiter=delimiter, encoding=encoding, on_bad_lines='warn', low_memory=False, na_values=['', 'NULL', 'null', 'N/A', 'n/a'])
        logger.info(f"✅ Loaded {len(df)} raw rows.")
    except Exception as e:
        logger.error(f"❌ CRITICAL: Failed to load data. Error: {e}")
        return None

    # 2. Remove summary rows
    logger.info("STEP 2: Removing summary rows...")
    def is_likely_header_row(row):
        row_str = ' '.join([str(val).lower() for val in row if pd.notna(val)])
        # More robust keyword check
        header_keywords = ['total', 'summary', 'average', 'count']
        return sum(1 for keyword in header_keywords if keyword in row_str) >= 2
    header_mask = df.apply(is_likely_header_row, axis=1)
    if header_mask.sum() > 0:
        df = df[~header_mask].copy()
        logger.info(f"✅ Removed {header_mask.sum()} summary rows.")

    # 3. Standardize Column Names
    logger.info("STEP 3: Standardizing column names...")
    def clean_column_name(col):
        name = str(col).strip().lower()
        name = re.sub(r'[^a-z0-9_]+', '_', name)
        return name.strip('_')
    df.columns = [clean_column_name(col) for col in df.columns]

    # 4. Extract Job ID
    logger.info("STEP 4: Extracting Job ID...")
    url_col = next((col for col in df.columns if 'url' in col or 'link' in col), None)
    if url_col:
        df['job_id'] = pd.to_numeric(df[url_col].str.extract(r'(\d+)', expand=False), errors='coerce')
        df.drop(columns=[c for c in df.columns if 'unnamed' in c or c == url_col], inplace=True, errors='ignore')
    else:
        df['job_id'] = np.nan

    # --- 5. Handle Missing Values (Revised Logic) ---
    logger.info("STEP 5: Handling missing values (conservative approach)...")
    # Only fill categorical columns where 'Unknown' is a safe default.
    # Leave other fields (like email, phone, financials) as NaN to avoid inventing data.
    CATEGORICAL_FILL_COLS = ['status_label', 'lead_status_label', 'customer_region', 'customer_rep', 'customer_lead_source']
    for col in df.columns:
        if col in CATEGORICAL_FILL_COLS:
            df[col].fillna('Unknown', inplace=True)
        # We no longer fill other string/object columns to avoid incorrect data.
        # We also no longer fill numeric columns with the median, to keep missing financials as NaN.
    logger.info("✅ Missing values handled conservatively.")

    # --- 6. Add Data Quality Flag (New) ---
    logger.info("STEP 6: Flagging high-quality rows...")
    # Define "good" data as per user requirements.
    required_cols = ['job_id', 'customer_first_name', 'customer_last_name', 'address']
    # Ensure all required columns exist before checking them.
    existing_required_cols = [col for col in required_cols if col in df.columns]
    if len(existing_required_cols) == len(required_cols):
        df['is_complete'] = df[existing_required_cols].notna().all(axis=1)
        logger.info(f"✅ Found {df['is_complete'].sum()} 'complete' rows based on required fields.")
    else:
        df['is_complete'] = False
        logger.warning("⚠️ Could not create 'is_complete' flag because one or more required columns were missing.")


    # --- 7. Optimize Data Types (Unchanged) ---
    logger.info("STEP 7: Optimizing data types...")
    for col in df.columns:
        if 'date' in col or 'time' in col:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception:
                logger.warning(f"Could not convert column '{col}' to datetime.")

    # --- 8. Remove Duplicates (Unchanged) ---
    logger.info("STEP 8: Removing duplicate rows...")
    if 'job_id' in df.columns:
        df.dropna(subset=['job_id'], inplace=True)
        # Convert job_id to integer type after dropping NaNs
        df['job_id'] = df['job_id'].astype(int)
        df.drop_duplicates(subset='job_id', keep='first', inplace=True)

    # --- 9. Add Calculated Fields for Looker (New) ---
    logger.info("STEP 9: Adding calculated fields for analytics...")

    # Create customer_full_name
    if 'customer_first_name' in df.columns and 'customer_last_name' in df.columns:
        df['customer_full_name'] = df['customer_first_name'].fillna('') + ' ' + df['customer_last_name'].fillna('')
        df['customer_full_name'] = df['customer_full_name'].str.strip()

    # Create is_insurance_claim
    if 'insurance_company_name' in df.columns:
        # A claim is considered an insurance claim if the company name is not null and not 'Unknown'
        df['is_insurance_claim'] = ~df['insurance_company_name'].isnull() & (df['insurance_company_name'] != 'Unknown')
    else:
        df['is_insurance_claim'] = False

    # Create job_duration_days
    if 'date_created' in df.columns and 'date_completed' in df.columns:
        # Ensure columns are datetime before subtraction
        df['job_duration_days'] = (df['date_completed'] - df['date_created']).dt.days
        # Fill missing durations with a neutral value like NaN.
        df['job_duration_days'].fillna(np.nan, inplace=True)

    logger.info("✅ Calculated fields added.")

    logger.info("=" * 60)
    logger.info(f"V2 CLEANING COMPLETE. Final shape: {df.shape}")
    logger.info("=" * 60)
    return df
