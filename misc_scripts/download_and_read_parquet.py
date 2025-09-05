"""
AWS S3 Parquet File Downloader and Reader

This script downloads parquet files from an S3 bucket to local storage
and reads them using pandas for analysis. It's designed for small to 
medium datasets that can fit in memory.

Requirements:
    - boto3: AWS SDK for Python
    - pandas: Data manipulation library
    - pyarrow: Parquet file support for pandas
    - AWS credentials configured (aws configure or environment variables)

Usage:
    python download_and_read_parquet.py
"""

import boto3
import pandas as pd
import os
from pathlib import Path

def main():
    """
    Main function to download and read parquet files from S3.
    
    Downloads all parquet files from the specified S3 bucket/prefix,
    reads them with pandas, and displays information about each file.
    """
    
    # Initialize S3 client using default credentials
    s3 = boto3.client('s3')
    
    # S3 configuration - modify these values as needed
    bucket = 'banking-analytics-dataset'  # S3 bucket name
    prefix = 'processed-data/customers_processed/'  # Path prefix in bucket
    local_dir = 'downloaded_parquet'  # Local directory to save files
    
    # Create local directory if it doesn't exist
    Path(local_dir).mkdir(exist_ok=True)
    
    print("Downloading parquet files from S3...")
    
    # Use paginator to handle large numbers of objects efficiently
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    parquet_files = []
    
    # Iterate through all pages of S3 objects
    for page in pages:
        # Check if the page contains any objects
        if 'Contents' in page:
            for obj in page['Contents']:
                # Only process parquet files
                if obj['Key'].endswith('.parquet'):
                    # Create local file path using just the filename
                    local_file = os.path.join(local_dir, os.path.basename(obj['Key']))
                    
                    # Download file from S3 to local storage
                    s3.download_file(bucket, obj['Key'], local_file)
                    parquet_files.append(local_file)
                    print(f"Downloaded: {obj['Key']}")
    
    # Exit if no parquet files were found
    if not parquet_files:
        print("No parquet files found!")
        exit()
    
    print(f"\nFound {len(parquet_files)} parquet files")
    
    # Read and analyze each parquet file
    print("\nReading parquet files...")
    all_data = []
    
    for file in parquet_files:
        # Read parquet file into pandas DataFrame
        df = pd.read_parquet(file)
        all_data.append(df)
        
        # Display file information
        print(f"\nFile: {file}")
        print(f"Shape: {df.shape}")  # (rows, columns)
        print(f"Columns: {list(df.columns)}")
        print("Sample data:")
        print(df.head())  # Show first 5 rows
    
    # Combine all DataFrames if multiple files exist
    if len(all_data) > 1:
        # Concatenate all DataFrames, resetting index
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"\nCombined data shape: {combined_df.shape}")
        print("Combined sample:")
        print(combined_df.head())

# Run the main function when script is executed directly
if __name__ == "__main__":
    main()