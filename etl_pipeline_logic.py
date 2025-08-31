# ================================================================
# COMPREHENSIVE ETL DATA CLEANING PIPELINE LOGIC
# Date: 2025-08-31
# Author: duketopceo (Refactored and Enhanced by Jules)
# Purpose: Core logic for data cleaning, with job_id extraction,
#          detailed logging, and modular functions for reporting.
# ================================================================

import pandas as pd
import numpy as np
import logging
import re
import warnings
import io

# --- Logging Configuration ---
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
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

# ================================================================
# PUBLIC FUNCTIONS
# ================================================================

def assess_raw_data(file_content, file_name):
    """
    Assesses raw file content to determine encoding and delimiter.
    """
    logger.info(f"ASSESSING: {file_name}")
    assessment = {
        'file_name': file_name,
        'issues_found': [],
        'working_encoding': 'utf-8',
        'likely_delimiter': ','
    }
    decoded_content = ""
    try:
        decoded_content = file_content.decode('utf-8')
    except UnicodeDecodeError:
        for encoding in ['latin-1', 'cp1252', 'iso-8859-1']:
            try:
                decoded_content = file_content.decode(encoding)
                assessment['working_encoding'] = encoding
                logger.info(f"Successfully decoded {file_name} with fallback encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
    if not decoded_content:
        logger.error(f"Failed to decode file: {file_name}")
        assessment['issues_found'].append("Failed to decode file.")
        return assessment
    lines = decoded_content.splitlines()
    comma_counts = [line.count(',') for line in lines[:20]]
    if comma_counts and max(comma_counts) > 0:
        assessment['likely_delimiter'] = ','
    try:
        pd.read_csv(io.StringIO(decoded_content), delimiter=assessment['likely_delimiter'], nrows=10)
    except Exception as e:
        logger.error(f"Pandas read error during assessment of {file_name}: {e}")
        assessment['issues_found'].append(f"Pandas read error: {e}")
    return assessment


def clean_csv_data(file_content, file_name, assessment_results=None):
    """
    Orchestrates the data cleaning process and generates a data quality report.

    Returns:
        tuple: A tuple containing:
            - pd.DataFrame: The cleaned dataframe.
            - pd.DataFrame: The data quality report dataframe.
    """
    logger.info("=" * 60)
    logger.info(f"STARTING DATA CLEANING FOR: {file_name}")
    logger.info("=" * 60)

    try:
        delimiter = assessment_results.get('likely_delimiter', ',') if assessment_results else ','
        encoding = assessment_results.get('working_encoding', 'utf-8') if assessment_results else 'utf-8'
        df_original = pd.read_csv(
            io.BytesIO(file_content),
            delimiter=delimiter,
            encoding=encoding,
            on_bad_lines='warn',
            low_memory=False,
            na_values=['', 'NULL', 'null', 'N/A', 'n/a']
        )
        df = df_original.copy()
        logger.info(f"Loaded {len(df)} raw rows from {file_name}.")
    except Exception as e:
        logger.error(f"CRITICAL: Failed to load data for {file_name}. Error: {e}")
        return None, None

    # --- Generate Initial Report Stats ---
    report = pd.DataFrame({
        'original_column': df.columns,
        'missing_percent_before': (df.isnull().sum() * 100 / len(df)).round(2)
    })

    # --- Cleaning Steps ---
    df, rows_removed = _remove_summary_rows(df)
    df, column_map = _standardize_column_names(df)
    df, job_id_cols_dropped = _extract_job_id(df)
    df, filled_info, dropped_info = _handle_missing_values(df)
    df, type_changes = _optimize_data_types(df)
    df, duplicates_removed = _remove_duplicates(df)

    # --- Compile Final Report ---
    report['cleaned_column'] = report['original_column'].map(column_map)

    actions = []
    details = []
    for index, row in report.iterrows():
        action = "Kept"
        detail = ""
        cleaned_col = row['cleaned_column']

        if cleaned_col in filled_info:
            action = "Filled"
            detail = f"Filled with {filled_info[cleaned_col]}"

        if cleaned_col in type_changes:
            detail += f" | Type changed to {type_changes[cleaned_col]}"
            detail = detail.strip(" | ")

        if cleaned_col not in df.columns:
            if cleaned_col in dropped_info:
                action = "Dropped"
                detail = f"Dropped (>90% missing)"
            elif cleaned_col in job_id_cols_dropped:
                action = "Dropped"
                detail = "Dropped after job_id extraction"
            else:
                action = "Dropped"
                detail = "Column was removed"

        actions.append(action)
        details.append(detail)

    report['action'] = actions
    report['details'] = details

    logger.info(f"CLEANING COMPLETE for {file_name}. Final shape: {df.shape}")
    return df, report

# ================================================================
# INTERNAL HELPER FUNCTIONS (MODIFIED TO RETURN ACTIONS)
# ================================================================

def _remove_summary_rows(df):
    def is_likely_header_row(row):
        row_str = ' '.join([str(val).lower() for val in row if pd.notna(val)])
        header_keywords = ['total', 'summary', 'average', 'count', 'subtotal']
        return sum(1 for keyword in header_keywords if keyword in row_str) >= 2
    header_mask = df.apply(is_likely_header_row, axis=1)
    num_removed = header_mask.sum()
    if num_removed > 0:
        logger.info(f"Removing {num_removed} likely summary/header rows.")
        df = df[~header_mask].copy()
    return df, num_removed

def _standardize_column_names(df):
    original_cols = df.columns.tolist()
    def clean_name(col):
        name = str(col).strip().lower()
        name = re.sub(r'[^a-z0-9_]+', '_', name)
        return name.strip('_')
    cleaned_cols = [clean_name(col) for col in original_cols]
    column_map = dict(zip(original_cols, cleaned_cols))
    df.columns = cleaned_cols
    logger.info("Standardized column names.")
    return df, column_map

def _extract_job_id(df):
    def find_url_column(d):
        for col in d.columns:
            if any(keyword in col.lower() for keyword in ['url', 'link', 'href']):
                return col
        return None
    url_col = find_url_column(df)
    cols_to_drop = []
    if url_col:
        logger.info(f"Found URL column: '{url_col}'. Extracting job_id.")
        df['job_id'] = df[url_col].astype(str).str.extract(r'(\d+)', expand=False)
        df['job_id'] = pd.to_numeric(df['job_id'], errors='coerce')
        cols_to_drop = [url_col] + [c for c in df.columns if 'unnamed' in c]
        df.drop(columns=cols_to_drop, inplace=True, errors='ignore')
        valid_ids = df['job_id'].notna().sum()
        logger.info(f"Successfully extracted {valid_ids} job IDs.")
    else:
        logger.warning("No URL column found. Cannot extract job_id.")
        df['job_id'] = np.nan
    return df, cols_to_drop

def _handle_missing_values(df):
    filled_info = {}
    dropped_info = []
    for col in df.columns:
        if col == 'job_id': continue
        missing_pct = df[col].isnull().mean() * 100
        if missing_pct > 90:
            df.drop(columns=[col], inplace=True)
            dropped_info.append(col)
            logger.warning(f"Dropped column '{col}' due to >90% missing values.")
        elif missing_pct > 0:
            num_missing = df[col].isnull().sum()
            fill_value = None
            if pd.api.types.is_numeric_dtype(df[col]):
                fill_value = df[col].median()
                filled_info[col] = f"median ({fill_value})"
            else:
                fill_value = df[col].mode().iloc[0] if not df[col].mode().empty else 'Unknown'
                filled_info[col] = f"mode ('{fill_value}')"
            df[col].fillna(fill_value, inplace=True)
            logger.info(f"Column '{col}': Filled {num_missing} missing values with {filled_info[col]}.")
    return df, filled_info, dropped_info

def _optimize_data_types(df):
    type_changes = {}
    for col in df.columns:
        original_type = df[col].dtype
        if original_type == 'object':
            df_converted = pd.to_numeric(df[col], errors='coerce')
            if not df_converted.isnull().all():
                df[col] = df_converted
                type_changes[col] = 'numeric'
                logger.info(f"Column '{col}': Converted to numeric type.")
                continue
        if df[col].dtype == 'object' and any(keyword in col.lower() for keyword in ['date', 'time']):
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                if df[col].dtype != original_type:
                    type_changes[col] = 'datetime'
                    logger.info(f"Column '{col}': Converted to datetime type.")
            except Exception:
                logger.warning(f"Could not convert column '{col}' to datetime.")
    return df, type_changes

def _remove_duplicates(df):
    num_duplicates = df.duplicated().sum()
    if num_duplicates > 0:
        df.drop_duplicates(inplace=True)
        logger.info(f"Removed {num_duplicates} duplicate rows.")
    return df, num_duplicates
