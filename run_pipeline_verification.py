import pandas as pd
from functools import reduce
import os

# Import the logic from our enhanced script
from etl_pipeline_logic import assess_raw_data, clean_csv_data

def run_verification():
    """
    Simulates the Jupyter Notebook workflow to run the ETL pipeline
    on the provided CSV files and verify the output.
    """
    print("--- STARTING PIPELINE VERIFICATION ---")

    # List of CSV files to process
    csv_files = [
        'boise_2024_data_raw.csv',
        'slc_data_partone_2024.csv',
        'slc_data_parttwo_2024.csv'
    ]

    # --- Simulate Step 3 & 4: Upload and Assess Data ---
    print("\nStep 1: Assessing files...")
    assessments = {}
    file_contents = {}
    for file_name in csv_files:
        if not os.path.exists(file_name):
            print(f"❌ ERROR: File not found: {file_name}")
            continue
        with open(file_name, 'rb') as f:
            file_content = f.read()
            file_contents[file_name] = file_content
            assessments[file_name] = assess_raw_data(file_content, file_name)
    print("✅ Assessment complete.")

    # --- Simulate Step 5: Clean and Merge Data ---
    print("\nStep 2: Cleaning and merging files...")
    cleaned_dfs = []
    for file_name, file_content in file_contents.items():
        print(f"\n--- Processing {file_name} ---")
        assessment = assessments.get(file_name)
        cleaned_df = clean_csv_data(file_content, file_name, assessment)

        if cleaned_df is not None and 'job_id' in cleaned_df.columns and cleaned_df['job_id'].notna().any():
            cleaned_dfs.append(cleaned_df)
            print(f"✅ Successfully cleaned {file_name}. Shape: {cleaned_df.shape}")
        else:
            print(f"⚠️ Warning: Could not clean or find job_ids in '{file_name}'. It will be excluded from the merge.")

    final_cleaned_df = None
    if not cleaned_dfs:
        print("\n❌ No valid dataframes were produced. Merge step skipped.")
    elif len(cleaned_dfs) == 1:
        final_cleaned_df = cleaned_dfs[0]
        print("\n✅ Only one valid dataframe. No merge needed.")
    else:
        print(f"\nAttempting to merge {len(cleaned_dfs)} cleaned dataframes...")
        try:
            # Set job_id as the index for all dataframes before combining.
            indexed_dfs = [df.set_index('job_id') for df in cleaned_dfs if 'job_id' in df.columns]

            # Use combine_first to intelligently merge, filling NaNs from one dataframe with data from the next.
            final_merged = indexed_dfs[0]
            for i in range(1, len(indexed_dfs)):
                final_merged = final_merged.combine_first(indexed_dfs[i])

            final_cleaned_df = final_merged.reset_index()
            print(f"✅ Successfully merged dataframes into a final dataset with shape {final_cleaned_df.shape}.")
        except Exception as e:
            print(f"❌ Error during merging: {e}")

    # --- Simulate Step 7: Export Cleaned Data ---
    print("\nStep 3: Exporting final data...")
    if final_cleaned_df is not None:
        output_filename = 'cleaned_rooflink_data.csv'
        try:
            final_cleaned_df.to_csv(output_filename, index=False)
            print(f"✅ Success! Cleaned data has been saved as '{output_filename}'.")
        except Exception as e:
            print(f"❌ Error saving file: {e}")
    else:
        print("No cleaned data available to export.")

    print("\n--- PIPELINE VERIFICATION COMPLETE ---")

if __name__ == "__main__":
    run_verification()
