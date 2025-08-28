import pandas as pd

# Load the cleaned CSV file
try:
    df = pd.read_csv('cleaned_rooflink_data.csv', low_memory=False)
    print("--- Data Types of Final Output File ---")
    # Print the data types of each column
    print(df.dtypes)
    print("\n--- Dataframe Info (including memory usage) ---")
    df.info(memory_usage='deep')
except FileNotFoundError:
    print("❌ Error: 'cleaned_rooflink_data.csv' not found.")
except Exception as e:
    print(f"❌ An error occurred: {e}")
