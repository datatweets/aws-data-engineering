"""
PySpark S3 Parquet Reader

This script reads parquet files directly from S3 using PySpark without 
downloading them locally. It's designed for large datasets that require 
distributed processing and can handle files that don't fit in memory.

Requirements:
    - pyspark: Apache Spark Python API
    - hadoop-aws: Hadoop AWS connector (automatically downloaded)
    - aws-java-sdk-bundle: AWS Java SDK (automatically downloaded)
    - AWS credentials configured (aws configure or environment variables)

Usage:
    python read_s3_parquet.py

Features:
    - Direct S3 access without local downloads
    - Distributed data processing
    - Automatic schema inference
    - Memory-efficient for large datasets
"""

from pyspark.sql import SparkSession
import os

def create_spark_session():
    """
    Create and configure a Spark session for reading from S3.
    
    Returns:
        SparkSession: Configured Spark session with S3 support
    """
    spark = SparkSession.builder \
        .appName("ReadS3Parquet") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .getOrCreate()
    
    return spark

def read_and_analyze_parquet(spark, s3_path):
    """
    Read parquet files from S3 and display analysis information.
    
    Args:
        spark (SparkSession): Active Spark session
        s3_path (str): S3 path to parquet files (s3a://bucket/path/)
    """
    try:
        # Read parquet files from S3 path and all subfolders
        # Spark automatically discovers and reads all parquet files in the path
        df = spark.read.parquet(s3_path)
        
        # Display sample data (default: 20 rows)
        print("Sample data from parquet files:")
        df.show()
        
        # Display schema information
        print("Data Schema:")
        df.printSchema()
        
        # Count total records (this triggers computation across all files)
        total_records = df.count()
        print(f"Total records: {total_records}")
        
        return df
        
    except Exception as e:
        print(f"Error reading parquet files: {e}")
        return None

def main():
    """
    Main function to execute the parquet reading workflow.
    """
    # S3 path configuration - modify as needed
    s3_path = "s3a://banking-analytics-dataset/processed-data/customers_processed/"
    
    # Create Spark session with S3 configuration
    spark = create_spark_session()
    
    try:
        # Read and analyze the parquet data
        df = read_and_analyze_parquet(spark, s3_path)
        
        if df is not None:
            print("Successfully processed parquet files from S3")
        else:
            print("Failed to process parquet files")
            
    finally:
        # Always stop Spark session to release resources
        spark.stop()
        print("Spark session stopped")

# Run the main function when script is executed directly
if __name__ == "__main__":
    main()