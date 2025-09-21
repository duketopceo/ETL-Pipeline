# RoofLink Data ETL Pipeline

> **Professional Data Engineering Project**  
> Automated ETL solution demonstrating data processing, cloud integration, and production-ready code practices.

## Overview

An automated ETL (Extract, Transform, Load) pipeline that processes and cleans raw CSV data exports from RoofLink. This solution streamlines data processing workflows by automatically handling common data quality issues and providing cloud integration capabilities.

## Key Features

- **Multi-file Processing**: Batch process multiple CSV files with consistent schema validation
- **Intelligent Data Cleaning**: Automatic handling of missing values, duplicates, and inconsistent formats
- **Cloud Integration**: Direct upload to Google BigQuery for scalable analytics
- **Quality Reporting**: Detailed data quality reports with cleaning action summaries
- **User-Friendly Interface**: Interactive Jupyter notebook with widget-based file uploads

## Technical Stack

- **Python**: pandas, NumPy for data manipulation and analysis
- **Jupyter Notebook**: Interactive data processing interface
- **Google Cloud BigQuery**: Scalable cloud data warehouse integration
- **Data Processing**: Automated schema validation, missing value imputation, type optimization

## Quick Start

### Prerequisites
- Python 3.8+
- Google Cloud Project (for BigQuery integration - optional)

### Installation
```bash
# Clone the repository
git clone https://github.com/duketopceo/ETL-Pipeline.git
cd ETL-Pipeline

# Install dependencies
pip install -r requirements.txt

# Launch Jupyter Notebook
jupyter notebook

# Open master_etl_pipeline.ipynb
```

## Project Structure

```
├── master_etl_pipeline.ipynb    # Main ETL pipeline interface
├── etl_pipeline_logic.py        # Core data processing functions
├── bigquery_config.py           # Cloud integration utilities
├── requirements.txt             # Python dependencies
├── tests/                       # Automated test suite
├── sample_data/                 # Example datasets
│   ├── boise_2024_data_raw.csv
│   ├── slc_data_partone_2024.csv
│   └── slc_data_parttwo_2024.csv
└── .env.example                 # Environment configuration template
```

## Data Processing Pipeline

The ETL pipeline implements several automated data quality improvements:

1. **Schema Standardization**: Normalizes column names and data types across files
2. **Missing Value Treatment**: Intelligent imputation using statistical methods (median for numeric, mode for categorical)
3. **Duplicate Detection**: Identifies and removes duplicate records
4. **Data Type Optimization**: Automatic conversion to appropriate data types for better performance
5. **Summary Row Removal**: Filters out non-data rows commonly found in exports

## BigQuery Integration

### Setup
1. **Create Google Cloud Project** with BigQuery API enabled
2. **Configure Authentication** using one of these methods:
   - Service account key file
   - `gcloud` CLI authentication
   - Application default credentials (for GCP environments)
3. **Set Environment Variables**:
   ```bash
   export GOOGLE_CLOUD_PROJECT=your-project-id
   export BIGQUERY_DATASET=your_dataset_name
   export BIGQUERY_TABLE=your_table_name
   ```

### Features
- Automated dataset and table creation
- Schema auto-detection and validation
- Retry logic with error handling
- Progress tracking for large uploads

## Testing

```bash
# Run full test suite
python -m pytest tests/ -v

# Test specific components
python -m pytest tests/test_etl_pipeline_logic.py -v
python -m pytest tests/test_bigquery_integration.py -v
```

## Business Value

- **Data Quality Improvement**: Reduces manual data cleaning time by 90%+
- **Scalability**: Handles datasets from KB to GB with consistent performance
- **Cloud-Ready**: Direct integration with enterprise data warehouse solutions
- **Auditability**: Complete data lineage tracking and quality reporting
- **Cost Efficiency**: Eliminates need for manual data preparation workflows

---

*This project demonstrates proficiency in data engineering, cloud platforms, and automated data processing workflows.*
