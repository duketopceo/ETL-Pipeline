# ================================================================
# COMPREHENSIVE ETL DATA CLEANING PIPELINE LOGIC (Corrected)
# Date: 2025-08-27 21:44:20 UTC
# Author: duketopceo (Adapted and Corrected by Jules)
# Purpose: Core logic for data cleaning, with job_id extraction and full cleaning rules.
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
    # This function was mostly correct and remains as is.
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
# DATA CLEANING AND TRANSFORMATION (Corrected and Enhanced)
# ================================================================
def clean_csv_data(file_content, file_name, assessment_results=None):
    """
    Cleans and transforms raw CSV data from a file-like object.

    This function performs a series of cleaning steps:
    1.  Loads data from file content.
    2.  Removes extraneous summary or header rows mixed in the data.
    3.  Standardizes all column names to a consistent format (snake_case).
    4.  Extracts a numeric 'job_id' from a URL column, which is critical for merging.
    5.  Performs data quality checks on the extracted job_id.
    6.  Handles missing values by dropping sparse columns and filling others.
    7.  Optimizes data types for memory efficiency.
    8.  Removes duplicate rows to ensure data integrity.

    Args:
        file_content (bytes): The raw byte content of the CSV file.
        file_name (str): The name of the file being processed.
        assessment_results (dict, optional): Pre-computed assessment details like encoding.

    Returns:
        pandas.DataFrame or None: A cleaned DataFrame, or None if a critical error occurs.
    """
    logger.info("=" * 60)
    logger.info(f"STARTING DATA CLEANING FOR: {file_name}")
    logger.info("=" * 60)

    # --- 1. Load Data ---
    # Load the raw data from the uploaded file content into a pandas DataFrame.
    # We use the encoding and delimiter detected in the assessment step for robustness.
    logger.info("STEP 1: Loading data from file...")
    try:
        delimiter = assessment_results.get('likely_delimiter', ',') if assessment_results else ','
        encoding = assessment_results.get('working_encoding', 'utf-8') if assessment_results else 'utf-8'
        df = pd.read_csv(io.BytesIO(file_content), delimiter=delimiter, encoding=encoding, on_bad_lines='warn', low_memory=False, na_values=['', 'NULL', 'null', 'N/A', 'n/a'])
        logger.info(f"✅ Loaded {len(df)} raw rows from {file_name}.")
    except Exception as e:
        logger.error(f"❌ CRITICAL: Failed to load data for {file_name}. Error: {e}")
        return None

    # --- 2. Remove extraneous header/footer rows ---
    # Some CSVs contain summary rows or repeated headers in the data. This function identifies
    # and removes them based on keywords often found in such rows.
    logger.info("STEP 2: Removing summary rows from data...")
    def is_likely_header_row(row):
        row_str = ' '.join([str(val).lower() for val in row if pd.notna(val)])
        header_keywords = ['total', 'summary', 'average', 'count']
        return sum(1 for keyword in header_keywords if keyword in row_str) >= 2

    header_mask = df.apply(is_likely_header_row, axis=1)
    if header_mask.sum() > 0:
        logger.info(f"✅ Found and removed {header_mask.sum()} summary/header rows.")
        df = df[~header_mask].copy()
    else:
        logger.info("✅ No summary rows found.")

    # --- 3. Standardize Column Names ---
    # Cleans column names by converting to lowercase, removing special characters,
    # and replacing spaces with underscores for easier access. e.g., "Customer / First Name" -> "customer_first_name"
    logger.info("STEP 3: Standardizing column names...")
    def clean_column_name(col):
        name = str(col).strip().lower()
        name = re.sub(r'[^a-z0-9_]+', '_', name)
        return name.strip('_')
    df.columns = [clean_column_name(col) for col in df.columns]
    logger.info("✅ Column names standardized.")

    # --- 4. Extract and Validate Job ID ---
    # The 'job_id' is the unique identifier for merging. It's extracted from a URL column.
    # This is a critical step, so we perform several checks.
    logger.info("STEP 4: Extracting and validating Job ID...")
    def find_url_column(d):
        for col in d.columns:
            if any(keyword in col for keyword in ['url', 'link', 'href']):
                return col
        return None

    url_col = find_url_column(df)
    if url_col:
        logger.info(f"Found URL column: '{url_col}'. Extracting job_id.")
        df['job_id'] = df[url_col].str.extract(r'(\d+)', expand=False)
        df['job_id'] = pd.to_numeric(df['job_id'], errors='coerce')

        # Drop the original url column and any unnamed columns that often appear
        cols_to_drop = [url_col] + [c for c in df.columns if 'unnamed' in c]
        df.drop(columns=cols_to_drop, inplace=True, errors='ignore')

        # Data Quality Check: Log missing or duplicate job_ids
        missing_ids = df['job_id'].isnull().sum()
        if missing_ids > 0:
            logger.warning(f"⚠️ Found {missing_ids} rows where job_id could not be extracted.")

        # Check for job_ids that are duplicated within this file
        duplicated_ids = df.job_id.dropna().duplicated().sum()
        if duplicated_ids > 0:
            logger.warning(f"⚠️ Found {duplicated_ids} duplicate job_ids within {file_name}. These will be handled in the duplicate removal step.")

        logger.info(f"✅ Successfully extracted {df['job_id'].notna().sum()} job IDs.")
    else:
        logger.warning("❌ No URL column found. Cannot extract job_id. This file may not be usable for merging.")
        df['job_id'] = np.nan # Ensure column exists but is empty

    # --- 5. Handle Missing Values ---
    # This section manages missing data with specific strategies.
    logger.info("STEP 5: Handling missing values...")

    # Data Quality Check: Log missing data in critical fields BEFORE filling them.
    CRITICAL_COLUMNS = ['customer_first_name', 'customer_last_name', 'address', 'city', 'state']
    for col in CRITICAL_COLUMNS:
        if col in df.columns and df[col].isnull().any():
            missing_rows = df[df[col].isnull()]
            for index, row in missing_rows.iterrows():
                job_id = row.get('job_id', 'N/A')
                logger.warning(f"⚠️ Missing critical data: Column '{col}' is empty for job_id: {job_id}.")

    for col in df.columns:
        if col == 'job_id': continue

        missing_pct = df[col].isnull().mean() * 100
        if missing_pct > 90:
            df.drop(columns=[col], inplace=True)
            logger.warning(f"Dropped column '{col}' due to >90% missing values.")
        elif missing_pct > 0:
            # For numeric columns, fill with the median to avoid skewing by outliers.
            if pd.api.types.is_numeric_dtype(df[col]):
                fill_value = df[col].median()
                df[col].fillna(fill_value, inplace=True)
            # For categorical columns, fill with the mode (most frequent value).
            else:
                fill_value = df[col].mode().iloc[0] if not df[col].mode().empty else 'Unknown'
                df[col].fillna(fill_value, inplace=True)
    logger.info("✅ Missing values handled.")

    # --- 6. Optimize Data Types ---
    # Convert columns to more memory-efficient types, like numeric or datetime where appropriate.
    logger.info("STEP 6: Optimizing data types...")
    for col in df.columns:
        if df[col].dtype == 'object':
            # Attempt to convert to numeric first
            try:
                df[col] = pd.to_numeric(df[col], errors='raise')
                continue # Move to next column if successful
            except (ValueError, TypeError):
                # If numeric conversion fails, check if it's a date column
                if any(keyword in col for keyword in ['date', 'time']):
                    try:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    except Exception as e:
                        logger.warning(f"⚠️ Could not convert date column '{col}' to datetime. Error: {e}")
    logger.info("✅ Data types optimized.")

    # --- 7. Remove Duplicate Rows ---
    # Remove rows that are complete duplicates across all columns.
    # Then, specifically ensure job_id is unique, keeping the first entry.
    logger.info("STEP 7: Removing duplicate rows...")
    num_full_duplicates = df.duplicated().sum()
    if num_full_duplicates > 0:
        df.drop_duplicates(inplace=True)
        logger.info(f"✅ Removed {num_full_duplicates} fully duplicate rows.")

    # Ensure job_id is unique, as it's the primary key for merging.
    if 'job_id' in df.columns:
        # We must handle NaNs in job_id before checking for duplicates
        df.dropna(subset=['job_id'], inplace=True)
        num_id_duplicates = df.job_id.duplicated().sum()
        if num_id_duplicates > 0:
            df.drop_duplicates(subset='job_id', keep='first', inplace=True)
            logger.warning(f"⚠️ Removed {num_id_duplicates} rows with duplicate job_ids, keeping the first instance of each.")

    logger.info("✅ Duplicate removal complete.")

    logger.info("=" * 60)
    logger.info(f"CLEANING COMPLETE for {file_name}. Final shape: {df.shape}")
    logger.info("=" * 60)
    return df

# ================================================================
# REPORTING (Unchanged)
# ================================================================
def generate_cleaning_report(df_original, df_cleaned, file_name):
    logger.info(f"GENERATING REPORT FOR: {file_name}")
    report = {'file_name': file_name, 'original_dataset': {}, 'cleaned_dataset': {}, 'improvements': {}}
    report['original_dataset'] = {'rows': len(df_original), 'columns': len(df_original.columns)}
    report['cleaned_dataset'] = {'rows': len(df_cleaned), 'columns': len(df_cleaned.columns)}
    report['improvements'] = {'rows_removed': report['original_dataset']['rows'] - report['cleaned_dataset']['rows']}
    return report
