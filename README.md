# ETL Pipeline for RoofLink Data

## Project Overview

This project provides a robust and user-friendly ETL (Extract, Transform, Load) pipeline for cleaning and processing raw data exported from RoofLink. The primary tool is a Jupyter Notebook (`master_etl_pipeline.ipynb`) that allows a user to upload multiple raw CSV files, which are then automatically cleaned, standardized, merged, and saved as a single output file.

The pipeline is designed to handle common data quality issues such as inconsistent column names, missing values, duplicate entries, and extraneous summary rows. It also generates a detailed Data Quality Report to give the user full visibility into the cleaning actions performed on their data.

## ✨ New Features & Bug Fixes

### 🔧 Recent Improvements
- **Fixed BigQuery integration bugs**: Proper error handling and authentication setup
- **Enhanced configuration management**: Persistent BigQuery settings with validation  
- **Improved authentication**: Support for multiple Google Cloud auth methods
- **Better error reporting**: Detailed error messages and troubleshooting guidance
- **Added comprehensive testing**: Full test coverage for BigQuery functionality
- **Security improvements**: Proper exception handling and input validation

## 🚀 Quick Start

### Prerequisites
1. Python 3.8+ with pip
2. For BigQuery integration: Google Cloud Project with BigQuery API enabled

### Installation
```bash
# Clone the repository
git clone https://github.com/duketopceo/ETL-Pipeline.git
cd ETL-Pipeline

# Install dependencies
pip install -r requirements.txt

# Start Jupyter Notebook
jupyter notebook

# Open master_etl_pipeline.ipynb and run the cells
```

## How to Use the ETL Notebook

To use the pipeline, you will need a Python environment with Jupyter Notebook installed, along with the required libraries (pandas, ipywidgets).

1.  **Launch Jupyter Notebook:** Open a terminal or command prompt, navigate to this project's directory, and run the command `jupyter notebook`.
2.  **Open the Notebook:** In the Jupyter interface that opens in your browser, click on `master_etl_pipeline.ipynb` to open it.
3.  **Run Cells Sequentially:** The notebook is organized into steps. Run each code cell in order from top to bottom by clicking on the cell and pressing `Shift + Enter`.
    *   **Step 1: Setup and Configuration:** This cell imports all necessary libraries and the core cleaning logic.
    *   **Step 2: Upload Your Raw Data:** Click the "Upload CSVs" button and select one or more raw CSV files from your computer.
    *   **Step 3: Process and Clean Uploaded Data:** This cell executes the main cleaning logic on your uploaded files.
    *   **Step 4: Merge Cleaned Data:** This cell merges the cleaned data from all uploaded files into a single dataset.
    *   **Step 5: Review Data Quality and Cleaning Report:** A detailed report is displayed, showing what actions were taken on every column of your original files.
    *   **Step 6: Save Cleaned Data to CSV:** The final, cleaned dataset is saved as `cleaned_merged_data.csv`.
    *   **Step 7: Review Detailed Logs:** A detailed technical log from the entire process is displayed.

## ☁️ Google Cloud BigQuery Integration

### Setup Instructions

#### Method 1: Service Account Key (Recommended for Development)
1. **Create a Service Account:**
   - Go to [Google Cloud Console](https://console.cloud.google.com) → IAM & Admin → Service Accounts
   - Click "Create Service Account"
   - Give it a name and description

2. **Add Required Permissions:**
   - Assign these roles to your service account:
     - `BigQuery Admin` (for full access)
     - OR specific permissions: `BigQuery Data Editor`, `BigQuery Job User`, `BigQuery Dataset Creator`

3. **Generate Key File:**
   - Click on your service account → Keys → Add Key → Create New Key
   - Choose JSON format and download the file
   - Store it securely (do NOT commit to version control)

4. **Configure Authentication:**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/keyfile.json"
   ```

#### Method 2: gcloud CLI (Recommended for Local Development)
1. **Install gcloud CLI:**
   - Follow instructions at https://cloud.google.com/sdk/docs/install

2. **Authenticate:**
   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   gcloud auth application-default login
   ```

#### Method 3: Application Default Credentials (Production)
- For Google Cloud Platform services (Compute Engine, Cloud Run, etc.)
- The service account is automatically attached to the resource
- No additional setup needed

### Configuration
1. **Copy the environment template:**
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` with your settings:**
   ```bash
   GOOGLE_CLOUD_PROJECT=your-project-id
   BIGQUERY_DATASET=your_dataset_name  
   BIGQUERY_TABLE=your_table_name
   ```

3. **Test your setup:** Run the authentication check in the notebook

### Required Permissions
Your service account or user account needs these BigQuery permissions:
- `bigquery.datasets.create` - Create datasets
- `bigquery.datasets.get` - Access existing datasets  
- `bigquery.tables.create` - Create tables
- `bigquery.tables.updateData` - Upload data
- `bigquery.jobs.create` - Run BigQuery jobs

### Troubleshooting
- **Authentication Issues:** Run the auth check in the notebook for detailed diagnostics
- **Permission Errors:** Verify your service account has the required BigQuery roles
- **Project Issues:** Ensure BigQuery API is enabled in your Google Cloud Project
- **Network Issues:** Check firewall settings if running in a restricted environment

## File Descriptions

*   `master_etl_pipeline.ipynb`: The main interactive Jupyter Notebook that serves as the user interface for the ETL pipeline.
*   `etl_pipeline_logic.py`: A Python script containing all the core functions for data assessment, cleaning, and transformation. The notebook imports its logic from this file.
*   `bigquery_config.py`: Configuration management and authentication helpers for Google Cloud BigQuery integration.
*   `requirements.txt`: List of required Python packages for the project.
*   `.env.example`: Template for environment configuration.
*   `tests/`: Test suite with comprehensive coverage including BigQuery functionality.
*   Sample data files:
    *   `boise_2024_data_raw.csv`: Sample raw data file for the Boise region.
    *   `slc_data_partone_2024.csv`: Sample raw data file (Part 1) for the Salt Lake City region.
    *   `slc_data_parttwo_2024.csv`: Sample raw data file (Part 2) for the Salt Lake City region.

## Instructions for AI Agents

This section provides context for AI developers working on this project.

*   **Primary Goal:** The project's main objective is to provide a user-friendly, notebook-based ETL tool for cleaning raw CSV data. The secondary, future goal is to adapt this logic for a large-scale, automated pipeline integrated with Google BigQuery.
*   **Core Architecture:** The project follows a two-part design:
    1.  **`master_etl_pipeline.ipynb` (Frontend/UI):** This notebook is the user-facing interface. It handles user interaction (like file uploads) and orchestrates the cleaning process.
    2.  **`etl_pipeline_logic.py` (Backend/Logic):** This script contains the pure data processing logic. It is designed to be independent of the notebook so it can be reused in other contexts (e.g., the future BigQuery pipeline).
    3.  **`bigquery_config.py` (Cloud Integration):** Helper functions for Google Cloud authentication and configuration management.
*   **Development Workflow:** When adding or modifying cleaning rules, changes should first be made in `etl_pipeline_logic.py`. The functions in this file should remain self-contained. Afterward, update `master_etl_pipeline.ipynb` to correctly call the new or modified functions and display any new results.
*   **Key Data Point:** The `job_id` is the primary key used to uniquely identify jobs and is used for merging datasets. This ID is extracted from a URL column in the raw data.
*   **Testing:** Run `pytest tests/` to execute the full test suite. Tests cover both core ETL functionality and BigQuery integration.

## 🔬 Testing

Run the test suite to validate functionality:

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test categories
python -m pytest tests/test_etl_pipeline_logic.py -v  # Core ETL tests
python -m pytest tests/test_bigquery_integration.py -v  # BigQuery tests
```

## Future Work

*   **BigQuery Integration:** ✅ **COMPLETED** - Full BigQuery integration with authentication, error handling, and configuration management.
*   **Advanced Schema Mapping:** Intelligent schema mapping between different data sources.
*   **Data Validation Rules:** Configurable validation rules for data quality checks.
*   **Automated Scheduling:** Integration with cloud scheduling services for automated pipeline runs.
*   **Advanced Logging:** Integration with cloud logging services for production monitoring.

## 📝 What You Need to Do

### For Basic Usage (CSV Processing Only):
- ✅ Nothing! Just run the notebook cells in order.

### For Google Cloud BigQuery Integration:
1. **Set up Google Cloud Project:**
   - Enable BigQuery API
   - Create or configure service account with appropriate permissions

2. **Choose Authentication Method:**
   - **Option A:** Download service account key file and set `GOOGLE_APPLICATION_CREDENTIALS`
   - **Option B:** Use `gcloud auth login` and `gcloud auth application-default login`
   - **Option C:** Use default credentials if running on Google Cloud Platform

3. **Configure Project Settings:**
   - Copy `.env.example` to `.env`
   - Set your `GOOGLE_CLOUD_PROJECT`, `BIGQUERY_DATASET`, and `BIGQUERY_TABLE`

4. **Test Integration:**
   - Run the authentication check cell in the notebook
   - Verify BigQuery upload functionality with sample data

### Development Setup:
1. Install development dependencies: `pip install -r requirements.txt`
2. Run tests: `python -m pytest tests/ -v`
3. Follow the coding patterns in `etl_pipeline_logic.py` for any new features

## 🔧 Technical Notes

- **Security:** Service account keys and `.env` files are automatically excluded from version control
- **Performance:** BigQuery uploads include retry logic and progress tracking
- **Compatibility:** Tested with Python 3.8+ and latest BigQuery client libraries
- **Error Handling:** Comprehensive error reporting with actionable guidance for resolution
