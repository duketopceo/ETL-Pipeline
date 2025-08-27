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
    logger.info("=" * 60)
    logger.info(f"STARTING DATA CLEANING FOR: {file_name}")
    logger.info("=" * 60)

    # --- Load Data ---
    try:
        delimiter = assessment_results.get('likely_delimiter', ',') if assessment_results else ','
        encoding = assessment_results.get('working_encoding', 'utf-8') if assessment_results else 'utf-8'
        df = pd.read_csv(io.BytesIO(file_content), delimiter=delimiter, encoding=encoding, on_bad_lines='warn', low_memory=False, na_values=['', 'NULL', 'null', 'N/A', 'n/a'])
        logger.info(f"Loaded {len(df)} raw rows.")
    except Exception as e:
        logger.error(f"CRITICAL: Failed to load data for {file_name}. Error: {e}")
        return None

    # --- Mixed Content Removal (Restored Logic) ---
    def is_likely_header_row(row):
        row_str = ' '.join([str(val).lower() for val in row if pd.notna(val)])
        header_keywords = ['total', 'summary', 'average', 'count']
        return sum(1 for keyword in header_keywords if keyword in row_str) >= 2

    header_mask = df.apply(is_likely_header_row, axis=1)
    if header_mask.sum() > 0:
        logger.info(f"Removing {header_mask.sum()} likely summary/header rows from data.")
        df = df[~header_mask].copy()

    # --- Column Name Standardization ---
    def clean_column_name(col):
        name = str(col).strip().lower()
        name = re.sub(r'[^a-z0-9_]+', '_', name)
        return name.strip('_')
    df.columns = [clean_column_name(col) for col in df.columns]

    # --- JOB ID EXTRACTION (CRITICAL FIX) ---
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
        logger.info(f"Successfully extracted {df['job_id'].notna().sum()} job IDs.")
    else:
        logger.warning("No URL column found. Cannot extract job_id.")
        df['job_id'] = np.nan # Ensure column exists but is empty

    # --- Handle Missing Values (Restored Logic) ---
    for col in df.columns:
        if col == 'job_id': continue
        missing_pct = df[col].isnull().mean() * 100
        if missing_pct > 90:
            df.drop(columns=[col], inplace=True)
            logger.warning(f"Dropped column '{col}' due to >90% missing values.")
        elif missing_pct > 0:
            if pd.api.types.is_numeric_dtype(df[col]):
                fill_value = df[col].median()
                df[col].fillna(fill_value, inplace=True)
            else:
                fill_value = df[col].mode().iloc[0] if not df[col].mode().empty else 'Unknown'
                df[col].fillna(fill_value, inplace=True)

    # --- Data Type Optimization (Restored Logic) ---
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                if any(keyword in col for keyword in ['date', 'time']):
                    df[col] = pd.to_datetime(df[col], errors='coerce')

    # --- Duplicate Removal ---
    num_duplicates = df.duplicated().sum()
    if num_duplicates > 0:
        df.drop_duplicates(inplace=True)
        logger.info(f"Removed {num_duplicates} duplicate rows.")

    logger.info(f"CLEANING COMPLETE for {file_name}. Final shape: {df.shape}")
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
