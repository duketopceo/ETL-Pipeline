# ETL Pipeline for RoofLink Data

## Project Overview

This project provides a robust and user-friendly ETL (Extract, Transform, Load) pipeline for cleaning and processing raw data exported from RoofLink. The primary tool is a Jupyter Notebook (`ETL_Pipeline.ipynb`) that allows a user to upload multiple raw CSV files, which are then automatically cleaned, standardized, merged, and saved as a single output file (`cleaned_merged_data.csv`).

The pipeline is designed to handle common data quality issues such as inconsistent column names, missing values, duplicate entries, and extraneous summary rows. It also generates a detailed Data Quality Report to give the user full visibility into the cleaning actions performed on their data.

## How to Use the ETL Notebook

To use the pipeline, you will need a Python environment with Jupyter Notebook installed, along with the required libraries (pandas, ipywidgets).

1.  **Launch Jupyter Notebook:** Open a terminal or command prompt, navigate to this project's directory, and run the command `jupyter notebook`.
2.  **Open the Notebook:** In the Jupyter interface that opens in your browser, click on `ETL_Pipeline.ipynb` to open it.
3.  **Run Cells Sequentially:** The notebook is organized into steps. Run each code cell in order from top to bottom by clicking on the cell and pressing `Shift + Enter`.
    *   **Step 1: Setup and Configuration:** This cell imports all necessary libraries and the core cleaning logic.
    *   **Step 2: Upload Your Raw Data:** Click the "Upload CSVs" button and select one or more raw CSV files from your computer.
    *   **Step 3: Process and Clean Uploaded Data:** This cell executes the main cleaning logic on your uploaded files.
    *   **Step 4: Merge Cleaned Data:** This cell merges the cleaned data from all uploaded files into a single dataset.
    *   **Step 5: Review Data Quality and Cleaning Report:** A detailed report is displayed, showing what actions were taken on every column of your original files.
    *   **Step 6: Save Cleaned Data to CSV:** The final, cleaned dataset is saved as `cleaned_merged_data.csv`.
    *   **Step 7: Review Detailed Logs:** A detailed technical log from the entire process is displayed.

## File Descriptions

*   `ETL_Pipeline.ipynb`: The main interactive Jupyter Notebook that serves as the user interface for the ETL pipeline.
*   `etl_pipeline_logic.py`: A Python script containing all the core functions for data assessment, cleaning, and transformation. The notebook imports its logic from this file.
*   `boise_2024_data_raw.csv`: Sample raw data file for the Boise region.
*   `slc_data_partone_2024.csv`: Sample raw data file (Part 1) for the Salt Lake City region.
*   `slc_data_parttwo_2024.csv`: Sample raw data file (Part 2) for the Salt Lake City region.
*   `etl_cleaning_log.txt`: A log file that is generated/overwritten each time the pipeline runs, containing detailed technical logs.

## Instructions for AI Agents

This section provides context for AI developers working on this project.

*   **Primary Goal:** The project's main objective is to provide a user-friendly, notebook-based ETL tool for cleaning raw CSV data. The secondary, future goal is to adapt this logic for a large-scale, automated pipeline integrated with Google BigQuery.
*   **Core Architecture:** The project follows a two-part design:
    1.  **`ETL_Pipeline.ipynb` (Frontend/UI):** This notebook is the user-facing interface. It handles user interaction (like file uploads) and orchestrates the cleaning process.
    2.  **`etl_pipeline_logic.py` (Backend/Logic):** This script contains the pure data processing logic. It is designed to be independent of the notebook so it can be reused in other contexts (e.g., the future BigQuery pipeline).
*   **Development Workflow:** When adding or modifying cleaning rules, changes should first be made in `etl_pipeline_logic.py`. The functions in this file should remain self-contained. Afterward, update `ETL_Pipeline.ipynb` to correctly call the new or modified functions and display any new results.
*   **Key Data Point:** The `job_id` is the primary key used to uniquely identify jobs and is used for merging datasets. This ID is extracted from a URL column in the raw data.

## Future Work

*   **BigQuery Integration:** The next major phase of this project is to adapt the cleaning logic for a cloud-based environment. The plan is to create a pipeline where data can be pulled from a source like Google Cloud Storage, processed by a BigQuery Notebook or Cloud Function using the logic from `etl_pipeline_logic.py`, and loaded directly into a BigQuery data warehouse.
