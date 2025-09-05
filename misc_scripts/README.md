# Misc Scripts - AWS Data Engineering

This folder contains utility scripts for reading and processing parquet data files from AWS S3.

## Scripts Overview

### 1. `download_and_read_parquet.py`
**Purpose:** Downloads parquet files from S3 to your local machine and reads them using pandas.

**What it does:**
- Connects to AWS S3 using boto3
- Downloads all parquet files from the `banking-analytics-dataset/processed-data/customers_processed/` bucket
- Saves files locally in a `downloaded_parquet` folder
- Reads each file using pandas
- Shows file information (shape, columns, sample data)
- Combines multiple files into one dataset if needed

**Best for:** Small to medium datasets that can fit in memory.

### 2. `read_s3_parquet.py`
**Purpose:** Reads parquet files directly from S3 using PySpark without downloading them locally.

**What it does:**
- Creates a Spark session configured to work with S3
- Reads parquet files directly from S3 using the s3a protocol
- Displays data schema and sample records
- Counts total number of records
- Automatically handles AWS credentials

**Best for:** Large datasets that require distributed processing.

## Prerequisites

### For both scripts:
- AWS credentials configured (via `aws configure` or environment variables)
- Access to the `banking-analytics-dataset` S3 bucket

### For `download_and_read_parquet.py`:
```bash
pip install boto3 pandas pyarrow
```

### For `read_s3_parquet.py`:
```bash
pip install pyspark
```

## Usage

### Download and Read Locally:
```bash
python download_and_read_parquet.py
```

### Read Directly from S3:
```bash
python read_s3_parquet.py
```

## Key Differences

| Feature | download_and_read_parquet.py | read_s3_parquet.py |
|---------|------------------------------|-------------------|
| Storage | Downloads files locally | Reads directly from S3 |
| Memory Usage | Loads all data into pandas | Distributed processing with Spark |
| Speed | Faster for small files | Better for large datasets |
| Dependencies | boto3, pandas | pyspark, hadoop-aws |
| Disk Space | Requires local storage | No local storage needed |

## Troubleshooting

**AWS Credentials Error:** Ensure your AWS credentials are configured with `aws configure` or set environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

**Permission Denied:** Make sure your AWS user has read access to the S3 bucket.