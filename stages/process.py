import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

# Define the input and output directories
input_dir = 'resources'
output_dir = 'brick'

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Get a list of all CSV files in the resources directory
csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]

# Convert each CSV file to Parquet
for csv_file in csv_files:
    # Construct full file paths
    csv_path = os.path.join(input_dir, csv_file)
    parquet_path = os.path.join(output_dir, f"{os.path.splitext(csv_file)[0]}.parquet")
    
    # Read the CSV file
    df = pd.read_csv(csv_path)
    
    # Convert to PyArrow Table
    table = pa.Table.from_pandas(df)
    
    # Write to Parquet
    pq.write_table(table, parquet_path)
    
    print(f"Converted {csv_file} to {parquet_path}")

print("Conversion complete!")