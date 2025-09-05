# PySpark DataFrames

## Table of Contents
- [PySpark DataFrames](#pyspark-dataframes)
  - [Table of Contents](#table-of-contents)
  - [Introduction: Why PySpark?](#introduction-why-pyspark)
    - [The Big Data Challenge](#the-big-data-challenge)
    - [What is Apache Spark?](#what-is-apache-spark)
    - [Why PySpark for Data Analysis?](#why-pyspark-for-data-analysis)
  - [Setting Up Your Environment](#setting-up-your-environment)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
    - [Project Structure](#project-structure)
  - [Chapter 1: Getting Started with Spark](#chapter-1-getting-started-with-spark)
    - [Script 01: Your First Spark Session](#script-01-your-first-spark-session)
  - [Chapter 2: Loading Data from Files](#chapter-2-loading-data-from-files)
    - [Script 02: Reading CSV Files](#script-02-reading-csv-files)
    - [Script 03: JSON and Parquet Files](#script-03-json-and-parquet-files)
    - [Script 04: Creating DataFrames from Python](#script-04-creating-dataframes-from-python)
  - [Chapter 3: Understanding DataFrame Schemas](#chapter-3-understanding-dataframe-schemas)
    - [Script 05: Schema Management](#script-05-schema-management)
  - [Chapter 4: Essential DataFrame Operations](#chapter-4-essential-dataframe-operations)
    - [Script 06: Basic DataFrame Operations](#script-06-basic-dataframe-operations)
  - [Chapter 5: Data Cleaning and Quality](#chapter-5-data-cleaning-and-quality)
    - [Script 07: Data Cleaning and Exploration](#script-07-data-cleaning-and-exploration)
  - [Chapter 6: String and Character Manipulation](#chapter-6-string-and-character-manipulation)
    - [Core String Functions](#core-string-functions)
  - [Chapter 7: Conditionals and Row/Column Operations](#chapter-7-conditionals-and-rowcolumn-operations)
    - [Basic and Complex Conditionals](#basic-and-complex-conditionals)
  - [Chapter 8: Data Sorting and Ranking](#chapter-8-data-sorting-and-ranking)
    - [Sorting and Ranking Operations](#sorting-and-ranking-operations)
  - [Chapter 9: Data Frequency, Counts and Unique Operations](#chapter-9-data-frequency-counts-and-unique-operations)
    - [Frequency Analysis and Counting](#frequency-analysis-and-counting)
  - [Chapter 10: Creating Unique Identifiers](#chapter-10-creating-unique-identifiers)
    - [Script 08: Indexing and Unique Identifiers](#script-08-indexing-and-unique-identifiers)
  - [Chapter 11: Complete Banking Analysis Lab](#chapter-11-complete-banking-analysis-lab)
    - [Script 09: Comprehensive Banking Analysis](#script-09-comprehensive-banking-analysis)
  - [Chapter 12: Working with Cloud Storage (S3)](#chapter-12-working-with-cloud-storage-s3)
    - [Script 10: S3 Parquet Reader](#script-10-s3-parquet-reader)
  - [Common Troubleshooting](#common-troubleshooting)
  - [Best Practices Summary](#best-practices-summary)
    - [Schema Management](#schema-management)
    - [DataFrame Operations](#dataframe-operations)
    - [Data Quality](#data-quality)
    - [Indexing](#indexing)
    - [Performance Tips](#performance-tips)
  - [Common Patterns and Examples](#common-patterns-and-examples)
    - [Pattern 1: Load → Clean → Transform → Save](#pattern-1-load--clean--transform--save)
    - [Pattern 2: Multi-Source Join](#pattern-2-multi-source-join)
    - [Pattern 3: Aggregation Pipeline](#pattern-3-aggregation-pipeline)
  - [Quick Reference Card](#quick-reference-card)
  - [Conclusion](#conclusion)

---

## Introduction: Why PySpark?

### The Big Data Challenge

Traditional tools like Excel or pandas work well for thousands of rows, but what happens when you have:
- 100 million transaction records
- 50 million customer profiles
- Years of historical data

Your computer runs out of memory. Processing takes hours. This is where PySpark shines.

### What is Apache Spark?

Apache Spark is a distributed computing framework that:
- Splits data across multiple computers
- Processes data in parallel
- Handles failures automatically
- Works 100x faster than traditional tools

### Why PySpark for Data Analysis?

PySpark combines:
- **Python's simplicity** - Familiar syntax for data scientists
- **Spark's power** - Distributed processing at scale
- **SQL support** - Use SQL queries on DataFrames
- **Rich ecosystem** - Machine learning, streaming, graph processing

---

## Setting Up Your Environment

### Prerequisites

```bash
# Check Python version (need 3.7+)
python --version

# Check Java (need Java 11)
java -version
```

### Installation

```bash
# Install PySpark
pip install pyspark==3.5.1

# Verify installation
python -c "import pyspark; print(pyspark.__version__)"
```

### Project Structure

Create this folder structure for the tutorial:

```
pyspark-tutorial/
├── scripts/
│   ├── script_01.py  # Spark session basics
│   ├── script_02.py  # Reading CSV files
│   ├── script_03.py  # JSON and Parquet
│   ├── script_04.py  # Creating from Python
│   ├── script_05.py  # Schema management
│   ├── script_06.py  # Basic operations
│   ├── script_07.py  # Data cleaning
│   ├── script_08.py  # Unique identifiers
│   ├── script_09.py  # Complete lab
│   └── script_10.py  # S3 operations
├── banking_dataset/
│   └── datasets/
│       ├── customers.csv
│       ├── accounts.csv
│       └── products.csv
└── output/
    └── (generated files)
```

---

## Chapter 1: Getting Started with Spark

### Script 01: Your First Spark Session

Every PySpark program starts by creating a Spark session.

```python
#!/usr/bin/env python3
"""
Script 01: Basic Spark Session Setup
From PySpark Tutorial - Setting up your first Spark session
"""

from pyspark.sql import SparkSession

def main():
    # Create a Spark session - this is like starting up Excel
    # The appName is just a friendly name for your work session
    spark = SparkSession.builder \
        .appName("My_First_PySpark_Program") \
        .getOrCreate()

    # Verify Spark is running
    print("Spark is ready to use!")
    print(f"Version: {spark.version}")
    
    # Stop the Spark session
    spark.stop()
    print("Spark session stopped successfully!")

if __name__ == "__main__":
    main()
```

**Key Concepts:**
- `SparkSession`: Entry point to all Spark functionality
- `appName`: Descriptive name for your application
- `getOrCreate()`: Creates new or gets existing session
- Always call `spark.stop()` to free resources

---

## Chapter 2: Loading Data from Files

### Script 02: Reading CSV Files

CSV is the most common data format in business.

```python
#!/usr/bin/env python3
"""
Script 02: Reading CSV Files
From PySpark Tutorial - Loading data from CSV files
"""

from pyspark.sql import SparkSession

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Reading_CSV_Files") \
        .getOrCreate()

    print("=== Reading CSV Files Example ===")
    
    try:
        # Reading a CSV file is straightforward
        # We'll use the banking dataset's customers file
        customers_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("banking_dataset/datasets/customers.csv")

        # The options we used:
        # - header="true" means the first row contains column names
        # - inferSchema="true" tells Spark to automatically detect data types

        # See what we loaded
        print("Our customer data has been loaded!")
        print(f"Number of customers: {customers_df.count()}")
        print(f"Number of columns: {len(customers_df.columns)}")

        # View the first 5 rows - like scrolling to the top in Excel
        print("\nFirst 5 customers:")
        customers_df.show(5)
        
        print("\nDataFrame schema:")
        customers_df.printSchema()
        
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        print("Make sure the banking_dataset/datasets/customers.csv file exists")
    
    # Stop Spark session
    spark.stop()
    print("\nScript completed successfully!")

if __name__ == "__main__":
    main()
```

### Script 03: JSON and Parquet Files

Different file formats for different needs.

```python
#!/usr/bin/env python3
"""
Script 03: Reading JSON and Parquet Files
From PySpark Tutorial - Working with different file formats
"""

from pyspark.sql import SparkSession
import json
import os

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Reading_JSON_Parquet_Files") \
        .getOrCreate()

    print("=== JSON and Parquet Files Example ===")
    
    # First, create a sample JSON file with product information
    print("1. Creating sample JSON file...")
    products_data = [
        {"product_id": "PROD_01", "name": "Basic Checking", "monthly_fee": 0},
        {"product_id": "PROD_02", "name": "Premium Savings", "monthly_fee": 10},
        {"product_id": "PROD_03", "name": "Student Account", "monthly_fee": 0}
    ]

    # Save to a JSON file (one JSON object per line - JSONL format)
    with open("products.json", "w") as file:
        for product in products_data:
            file.write(json.dumps(product) + "\n")
    
    print("Sample JSON file created: products.json")

    try:
        # Now read it with PySpark
        print("\n2. Reading JSON file...")
        products_df = spark.read.json("products.json")

        print("Products loaded from JSON:")
        products_df.show()

        # JSON files are special - Spark can automatically understand their structure
        print("Notice how Spark automatically detected the column names and types!")
        products_df.printSchema()

        # Read customers CSV for Parquet example
        print("\n3. Working with Parquet files...")
        customers_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("banking_dataset/datasets/customers.csv")
        
        # First, save our customers DataFrame as Parquet
        customers_df.write \
            .mode("overwrite") \
            .parquet("customers_parquet")

        print("Saved customers data as Parquet format")

        # Now read it back
        customers_parquet = spark.read.parquet("customers_parquet")

        print("Reading Parquet is much faster than CSV for large files!")
        customers_parquet.show(5)

        # Parquet files maintain data types perfectly - no need to specify schema
        print("\nParquet schema (notice data types are preserved):")
        customers_parquet.printSchema()
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Clean up created files
        if os.path.exists("products.json"):
            os.remove("products.json")
            print("\nCleaned up: products.json")
    
    # Stop Spark session
    spark.stop()
    print("\nScript completed successfully!")

if __name__ == "__main__":
    main()
```

### Script 04: Creating DataFrames from Python

Sometimes you need to create DataFrames from Python data.

```python
#!/usr/bin/env python3
"""
Script 04: Creating DataFrames from Python Data
From PySpark Tutorial - Converting Python lists/dicts to DataFrames
"""

from pyspark.sql import SparkSession

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Creating_DataFrames_from_Python") \
        .getOrCreate()

    print("=== Creating DataFrames from Python Data ===")
    
    print("1. Method 1: From a list of tuples")
    # Method 1: From a list of tuples
    branch_data = [
        ("BR_001", "Downtown Branch", "New York", 1000000.00),
        ("BR_002", "Airport Branch", "Los Angeles", 750000.00),
        ("BR_003", "Mall Branch", "Chicago", 500000.00)
    ]

    # Specify column names
    columns = ["branch_id", "branch_name", "city", "total_deposits"]

    # Create the DataFrame
    branches_df = spark.createDataFrame(branch_data, columns)

    print("Created DataFrame from Python list:")
    branches_df.show()
    print("Schema:")
    branches_df.printSchema()

    print("\n2. Method 2: From a list of dictionaries (more readable)")
    # Method 2: From a list of dictionaries
    account_types = [
        {"type_id": 1, "type_name": "Checking", "minimum_balance": 100},
        {"type_id": 2, "type_name": "Savings", "minimum_balance": 500},
        {"type_id": 3, "type_name": "Money Market", "minimum_balance": 2500}
    ]

    account_types_df = spark.createDataFrame(account_types)
    print("Created DataFrame from dictionaries:")
    account_types_df.show()
    print("Schema:")
    account_types_df.printSchema()

    print("\n3. Example showing data type handling")
    # ZIP codes with leading zeros
    sample_data = [
        ("John", "01234"),  # ZIP code starting with 0
        ("Sarah", "90210"),
        ("Mike", "00501")   # Another ZIP starting with 0
    ]

    # Without specifying schema, Spark treats ZIP as string by default
    df_with_zip = spark.createDataFrame(sample_data, ["name", "zip_code"])
    print("ZIP codes preserved as strings:")
    df_with_zip.show()
    df_with_zip.printSchema()

    print("\n4. S3 Reading Example (pattern only - won't run without AWS setup)")
    print("# Example of reading from S3:")
    print("# customers_s3 = spark.read \\")
    print("#     .option('header', 'true') \\")
    print("#     .option('inferSchema', 'true') \\")
    print("#     .csv('s3://my-bucket/banking-data/customers.csv')")
    
    # Stop Spark session
    spark.stop()
    print("\nScript completed successfully!")

if __name__ == "__main__":
    main()
```

---

## Chapter 3: Understanding DataFrame Schemas

### Script 05: Schema Management

Schemas define the structure of your data.

```python
#!/usr/bin/env python3
"""
Script 05: Schema Management
From PySpark Tutorial - Understanding and defining DataFrame schemas
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, 
    BooleanType, DateType, TimestampType, LongType
)
from pyspark.sql.functions import col

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Schema_Management") \
        .getOrCreate()

    print("=== DataFrame Schema Management ===")
    
    try:
        print("1. Loading data with inferred schema")
        # Load customers data with inferred schema
        customers_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("banking_dataset/datasets/customers.csv")

        # View the schema - this shows the structure of our data
        print("Customer DataFrame Schema (inferred):")
        customers_df.printSchema()
        
        print("\n2. Defining explicit schemas")
        # Method 1: Define a detailed schema
        customer_schema = StructType([
            StructField("customer_id", StringType(), False),  # False means cannot be null
            StructField("first_name", StringType(), True),    # True means can be null
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("risk_score", IntegerType(), True),
            StructField("annual_income", DoubleType(), True),
            StructField("employment_status", StringType(), True)
        ])

        # Now read the CSV with our explicit schema
        customers_with_schema = spark.read \
            .option("header", "true") \
            .schema(customer_schema) \
            .csv("banking_dataset/datasets/customers.csv")

        print("With explicit schema:")
        customers_with_schema.printSchema()

        print("\n3. Simple schema string method")
        # Method 2: Simple schema string (easier for basic cases)
        simple_schema = "customer_id STRING, first_name STRING, last_name STRING, annual_income DOUBLE"

        customers_simple_schema = spark.read \
            .option("header", "true") \
            .schema(simple_schema) \
            .csv("banking_dataset/datasets/customers.csv")

        print("With simple schema string:")
        customers_simple_schema.printSchema()
        customers_simple_schema.select("customer_id", "first_name", "annual_income").show(5)

        print("\n4. Common PySpark data types demonstration")
        # Create a sample accounts schema showing different data types
        account_schema = StructType([
            StructField("account_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("balance", DoubleType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("open_date", DateType(), True),
            StructField("daily_limit", IntegerType(), True),
            StructField("account_number", LongType(), True)
        ])

        print("Account schema with various data types:")
        for field in account_schema.fields:
            print(f"  {field.name}: {field.dataType} (nullable: {field.nullable})")

        print("\n5. Data type casting examples")
        # Cast examples
        customers_casted = customers_df \
            .withColumn("annual_income", col("annual_income").cast(DoubleType())) \
            .withColumn("risk_score", col("risk_score").cast(IntegerType()))

        print("After casting columns to specific types:")
        customers_casted.printSchema()

        # You can also cast to string when needed
        customers_with_string_income = customers_casted \
            .withColumn("income_as_text", col("annual_income").cast(StringType()))

        print("\nCasting numeric to string:")
        customers_with_string_income.select("annual_income", "income_as_text").show(5)

        print("\n6. Data types explanation:")
        print("- StringType(): For text data (names, addresses, IDs, emails)")
        print("- IntegerType(): For whole numbers (-2,147,483,648 to 2,147,483,647)")
        print("- LongType(): For large whole numbers")
        print("- DoubleType(): For decimal numbers (high precision)")
        print("- BooleanType(): For true/false values")
        print("- DateType(): For dates (without time)")
        print("- TimestampType(): For date and time")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure the banking_dataset/datasets/customers.csv file exists")
    
    # Stop Spark session
    spark.stop()
    print("\nScript completed successfully!")

if __name__ == "__main__":
    main()
```

---

## Chapter 4: Essential DataFrame Operations

### Script 06: Basic DataFrame Operations

Core operations you'll use every day.

```python
#!/usr/bin/env python3
"""
Script 06: Basic DataFrame Operations
From PySpark Tutorial - Fundamental operations for data manipulation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, concat_ws, lit

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Basic_DataFrame_Operations") \
        .getOrCreate()

    print("=== Basic DataFrame Operations ===")
    
    try:
        # Load our customers data
        customers_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("banking_dataset/datasets/customers.csv")

        print(f"Loaded {customers_df.count()} customer records")

        print("\n1. Selecting Columns")
        # Method 1: Select specific columns by name
        basic_info = customers_df.select("customer_id", "first_name", "last_name")
        print("Basic customer information:")
        basic_info.show(5)

        # Method 2: Select and rename columns
        customer_summary = customers_df.select(
            col("customer_id"),
            col("first_name").alias("fname"),  # alias renames the column
            col("last_name").alias("lname"),
            col("annual_income").alias("yearly_income")
        )
        print("\nCustomer summary with renamed columns:")
        customer_summary.show(5)

        # Method 3: Select all columns except some
        all_columns = customers_df.columns
        columns_to_keep = [c for c in all_columns if c not in ["phone", "address"]]
        customers_no_contact = customers_df.select(columns_to_keep)
        print(f"\nSelected {len(columns_to_keep)} out of {len(all_columns)} columns")

        print("\n2. Filtering Rows")
        # Simple filter - find high-income customers
        high_income = customers_df.filter(col("annual_income") > 100000)
        print(f"Customers with income > $100,000: {high_income.count()}")
        high_income.select("customer_id", "first_name", "last_name", "annual_income").show(5)

        # Multiple conditions - use & for AND, | for OR
        premium_safe = customers_df.filter(
            (col("customer_segment") == "Premium") & 
            (col("risk_score") > 700)
        )
        print(f"\nPremium customers with risk score > 700: {premium_safe.count()}")

        # SQL-style WHERE clauses
        specific_states = customers_df.where("state IN ('IL', 'NY', 'CA')")
        print(f"\nCustomers in IL, NY, or CA: {specific_states.count()}")

        print("\n3. Adding New Columns")
        # Add a simple calculated column
        customers_with_full_name = customers_df.withColumn(
            "full_name", 
            concat_ws(" ", col("first_name"), col("last_name"))
        )

        print("Added full_name column:")
        customers_with_full_name.select("first_name", "last_name", "full_name").show(5)

        # Add a column with simple conditions
        customers_with_income = customers_with_full_name.withColumn(
            "monthly_income",
            col("annual_income") / 12
        )

        print("\nAdded monthly income:")
        customers_with_income.select("annual_income", "monthly_income").show(5)

        # Add a constant value column
        customers_with_bank = customers_with_income.withColumn(
            "bank_name",
            lit("First National Bank")  # lit() creates a literal value
        )

        print("\nAdded constant column:")
        customers_with_bank.select("customer_id", "bank_name").show(5)

        print("\n4. Dropping and Renaming Columns")
        # Drop single column
        customers_no_phone = customers_df.drop("phone")
        print(f"After dropping 'phone': {len(customers_no_phone.columns)} columns")

        # Drop multiple columns
        customers_minimal = customers_df.drop("phone", "address", "city", "state", "zip_code")
        print("Minimal customer data columns:", customers_minimal.columns)

        # Rename columns
        customers_renamed = customers_minimal \
            .withColumnRenamed("customer_id", "cust_id") \
            .withColumnRenamed("first_name", "fname") \
            .withColumnRenamed("last_name", "lname")

        print("\nRenamed columns:")
        customers_renamed.show(5)

        print("\n5. Sorting Data")
        # Sort by single column
        sorted_by_income = customers_df.orderBy("annual_income", ascending=False)
        print("Top 5 highest income customers:")
        sorted_by_income.select("customer_id", "first_name", "last_name", "annual_income").show(5)

        # Sort by multiple columns
        sorted_complex = customers_df.orderBy(
            col("customer_segment"),
            col("annual_income").desc()  # desc() for descending order
        )
        print("\nSorted by segment, then income (descending):")
        sorted_complex.select("customer_segment", "annual_income", "first_name").show(10)
        
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure the banking_dataset/datasets/customers.csv file exists")
    
    # Stop Spark session
    spark.stop()
    print("\nScript completed successfully!")

if __name__ == "__main__":
    main()
```

---

## Chapter 5: Data Cleaning and Quality

### Script 07: Data Cleaning and Exploration

Real data is messy - here's how to clean it.

```python
#!/usr/bin/env python3
"""
Script 07: Data Cleaning and Exploration
From PySpark Tutorial - Handling missing data and basic exploration
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, min, max, trim, initcap, lower

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Data_Cleaning_Exploration") \
        .getOrCreate()

    print("=== Data Cleaning and Exploration ===")
    
    try:
        # Load our customers data
        customers_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("banking_dataset/datasets/customers.csv")

        print(f"Loaded {customers_df.count()} customer records")

        print("\n1. Handling Missing Data")
        # Check for null values in each column
        print("Checking for missing data:")
        null_counts = customers_df.select(
            [count(col(c).isNull().cast("int")).alias(c) for c in customers_df.columns]
        )
        null_counts.show()

        # Drop rows with ANY null values
        customers_complete = customers_df.dropna()
        print(f"\nRows after removing ANY nulls: {customers_complete.count()}")

        # Drop rows where specific columns are null
        customers_with_email = customers_df.dropna(subset=["email"])
        print(f"Rows with email present: {customers_with_email.count()}")

        # Fill null values with defaults
        customers_filled = customers_df.fillna({
            "phone": "No Phone",
            "risk_score": 500,  # Default risk score
            "annual_income": 0
        })

        print("Filled null values with defaults")
        print("Sample after filling nulls:")
        customers_filled.select("phone", "risk_score", "annual_income").show(5)

        print("\n2. Data Cleaning - Text Standardization")
        # Clean the data - trim whitespace and standardize case
        print("Before cleaning (notice potential whitespace):")
        customers_df.select("customer_id", "first_name", "last_name").show(5, truncate=False)

        customers_clean = customers_df \
            .withColumn("first_name", initcap(trim(col("first_name")))) \
            .withColumn("last_name", initcap(trim(col("last_name")))) \
            .withColumn("email", lower(trim(col("email")))) \
            .withColumn("city", initcap(trim(col("city"))))

        print("\nAfter cleaning:")
        customers_clean.select("customer_id", "first_name", "last_name", "email").show(5, truncate=False)

        print("\n3. Basic Data Exploration")
        # Basic statistics for numeric columns
        print("Statistical summary of customer data:")
        customers_df.describe().show()

        # Count unique values in a column
        unique_segments = customers_df.select("customer_segment").distinct().count()
        print(f"\nNumber of unique customer segments: {unique_segments}")

        # Show the unique values
        print("Customer segments:")
        customers_df.select("customer_segment").distinct().show()

        # Group by and count - like a pivot table in Excel
        segment_counts = customers_df.groupBy("customer_segment").count()
        print("\nCustomers per segment:")
        segment_counts.orderBy("count", ascending=False).show()

        print("\n4. Advanced Grouping and Aggregation")
        # Calculate averages by group
        segment_stats = customers_df.groupBy("customer_segment").agg(
            count("*").alias("customer_count"),
            avg("annual_income").alias("avg_income"),
            min("annual_income").alias("min_income"),
            max("annual_income").alias("max_income"),
            avg("risk_score").alias("avg_risk_score")
        )

        print("Income statistics by segment:")
        segment_stats.orderBy("avg_income", ascending=False).show()

        print("\n5. State Analysis")
        # State-wise analysis
        state_analysis = customers_df.groupBy("state").agg(
            count("*").alias("customer_count"),
            avg("annual_income").alias("avg_income")
        ).orderBy("customer_count", ascending=False)

        print("Top states by customer count:")
        state_analysis.show(10)

        print("\n6. Data Quality Insights")
        # Check for potential data quality issues
        print("Data quality checks:")
        
        # Check for unusually high or low incomes
        unusual_incomes = customers_df.filter(
            (col("annual_income") < 10000) | (col("annual_income") > 500000)
        )
        print(f"Customers with unusual incomes (<$10K or >$500K): {unusual_incomes.count()}")
        
        # Check for extreme risk scores
        extreme_risk = customers_df.filter(
            (col("risk_score") < 300) | (col("risk_score") > 850)
        )
        print(f"Customers with extreme risk scores (<300 or >850): {extreme_risk.count()}")

        # Email format validation (basic check)
        invalid_emails = customers_df.filter(~col("email").contains("@"))
        print(f"Potentially invalid email addresses: {invalid_emails.count()}")

        if invalid_emails.count() > 0:
            print("Sample invalid emails:")
            invalid_emails.select("customer_id", "email").show(5)
        
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure the banking_dataset/datasets/customers.csv file exists")
    
    # Stop Spark session
    spark.stop()
    print("\nScript completed successfully!")

if __name__ == "__main__":
    main()
```

---

## Chapter 6: String and Character Manipulation

### Core String Functions

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("StringManipulation").getOrCreate()

# Load data
df = spark.read.option("header", "true").csv("banking_dataset/datasets/customers.csv")

# STRING TRIMMING
# Remove leading/trailing spaces
df_trimmed = df.withColumn("first_name_clean", trim(col("first_name")))
df_trimmed = df_trimmed.withColumn("last_name_clean", ltrim(col("last_name")))  # Left trim only
df_trimmed = df_trimmed.withColumn("email_clean", rtrim(col("email")))  # Right trim only

# STRING REPLACEMENT
# Replace characters in strings
df_replaced = df.withColumn("phone_formatted",
    regexp_replace(col("phone"), "-", "")  # Remove dashes
)

df_replaced = df_replaced.withColumn("address_clean",
    translate(col("address"), ".,", "")  # Remove dots and commas
)

# STRING CONCATENATION
# Combine multiple columns
df_with_full_name = df.withColumn(
    "full_name",
    concat_ws(" ", col("first_name"), col("last_name"))
)

df_with_full_address = df_with_full_name.withColumn(
    "full_address",
    concat_ws(", ", col("address"), col("city"), col("state"), col("zip_code"))
)

# STRING FORMATTING
# Case conversions
df_case = df.withColumn("first_name_upper", upper(col("first_name"))) \
            .withColumn("last_name_lower", lower(col("last_name"))) \
            .withColumn("city_title", initcap(col("city")))  # Title case

# ADVANCED STRING OPERATIONS
# Substring extraction
df_substr = df.withColumn("state_code", substring(col("state"), 1, 2))

# Split strings
df_split = df_with_full_name.withColumn("name_parts", split(col("full_name"), " "))

# String length
df_length = df.withColumn("email_length", length(col("email")))

# Padding strings
df_padded = df.withColumn("customer_id_padded", lpad(col("customer_id"), 10, "0"))

# Format string with template (uses the full_name we created above)
df_formatted = df_with_full_name.withColumn(
    "customer_info",
    format_string("%s from %s", col("full_name"), col("state"))
)

# Lab: Customer Data Standardization
print("=== Customer Data Standardization Lab ===")

# Complete standardization pipeline
customers_standardized = df \
    .withColumn("first_name", initcap(trim(col("first_name")))) \
    .withColumn("last_name", initcap(trim(col("last_name")))) \
    .withColumn("email", lower(trim(col("email")))) \
    .withColumn("phone", regexp_replace(col("phone"), "[^0-9]", "")) \
    .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name"))) \
    .withColumn(
        "customer_code",
        concat(
            upper(substring(col("first_name"), 1, 3)),
            upper(substring(col("last_name"), 1, 3)),
            lpad(col("customer_id"), 5, "0")
        )
    )

customers_standardized.select("customer_code", "full_name", "email", "phone").show(10)

spark.stop()
```

---

## Chapter 7: Conditionals and Row/Column Operations

### Basic and Complex Conditionals

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Conditionals").getOrCreate()

# Load data
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("banking_dataset/datasets/customers.csv")

# BASIC CONDITIONALS
# Simple if-then-else logic
df_income = df.withColumn("income_category",
    when(col("annual_income") < 30000, "Low")
    .when(col("annual_income") < 60000, "Medium")
    .when(col("annual_income") < 100000, "High")
    .otherwise("Very High")
)

# COMPLEX CASE LOGIC
# Multiple conditions combined
df_risk = df.withColumn("risk_flag",
    when((col("risk_score") < 600) & (col("annual_income") < 30000), "High Risk")
    .when((col("risk_score") < 600) | (col("annual_income") < 20000), "Medium Risk")
    .when(col("risk_score") >= 750, "Low Risk")
    .otherwise("Standard Risk")
)

# LOGICAL OPERATORS
# Combining conditions with and, or, not
df_filtered = df.filter(
    (col("customer_segment") == "Premium") & 
    (~col("email").isNull()) &  # Not null
    (col("risk_score") > 700)
)

# BOOLEAN OPERATIONS
# Check for nulls
df_nulls = df.withColumn("has_email", col("email").isNotNull())
df_nulls = df_nulls.withColumn("missing_phone", col("phone").isNull())

# Check if value in list
valid_states = ["CA", "NY", "TX", "FL", "IL"]
df_states = df.withColumn("is_major_state", col("state").isin(valid_states))

# ROW-WISE CALCULATIONS
# Calculate across columns
df_calc = df.withColumn("monthly_income", col("annual_income") / 12)
df_calc = df_calc.withColumn("risk_adjusted_income",
    col("annual_income") * (col("risk_score") / 1000))

# COLUMN-WISE OPERATIONS
# Add multiple calculated columns at once
df_enhanced = df.select(
    "*",  # All original columns
    (col("annual_income") / 12).alias("monthly_income"),
    (col("risk_score") / 100).alias("risk_percentage"),
    when(col("risk_score") < 600, True).otherwise(False).alias("needs_review")
)

# Lab: Risk Flagging and Conditional Calculations
print("=== Risk Flagging Lab ===")

# Comprehensive risk assessment
df_risk_analysis = df.withColumn("risk_level",
    when(col("risk_score") < 500, 5)  # Highest risk
    .when(col("risk_score") < 600, 4)
    .when(col("risk_score") < 700, 3)
    .when(col("risk_score") < 800, 2)
    .otherwise(1)  # Lowest risk
).withColumn("income_level",
    when(col("annual_income") < 25000, 1)
    .when(col("annual_income") < 50000, 2)
    .when(col("annual_income") < 75000, 3)
    .when(col("annual_income") < 100000, 4)
    .otherwise(5)
).withColumn("combined_score",
    col("risk_level") + col("income_level")
).withColumn("action_required",
    when(col("combined_score") >= 8, "Immediate Review")
    .when(col("combined_score") >= 6, "Monitor")
    .otherwise("No Action")
)

# Show results
df_risk_analysis.select(
    "customer_id", "risk_score", "annual_income", 
    "risk_level", "income_level", "combined_score", "action_required"
).show(20)

spark.stop()
```

---

## Chapter 8: Data Sorting and Ranking

### Sorting and Ranking Operations

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SortingRanking").getOrCreate()

# Load data
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("banking_dataset/datasets/customers.csv")

# SINGLE AND MULTI-COLUMN SORTING
# Sort by single column
df_sorted = df.orderBy("annual_income")  # Ascending by default
df_sorted_desc = df.orderBy(col("annual_income").desc())  # Descending

# Sort by multiple columns
df_multi_sort = df.orderBy(
    col("customer_segment").asc(),
    col("annual_income").desc(),
    col("risk_score").desc()
)

print("Top 10 by income within each segment:")
df_multi_sort.select("customer_segment", "annual_income", "risk_score").show(10)

# RANKING FUNCTIONS
# Create window specification
windowSpec = Window.partitionBy("customer_segment").orderBy(col("annual_income").desc())

# Row number - unique sequential number
df_ranked = df.withColumn("row_num", row_number().over(windowSpec))

# Rank - allows ties, gaps in sequence
df_ranked = df_ranked.withColumn("income_rank", rank().over(windowSpec))

# Dense rank - allows ties, no gaps
df_ranked = df_ranked.withColumn("dense_rank", dense_rank().over(windowSpec))

print("Ranking comparison:")
df_ranked.select(
    "customer_segment", "annual_income", 
    "row_num", "income_rank", "dense_rank"
).filter(col("customer_segment") == "Premium").show(10)

# FIRST/LAST N ROWS
# Get top N rows
top_10_customers = df.orderBy(col("annual_income").desc()).limit(10)
print("Top 10 highest income customers:")
top_10_customers.select("customer_id", "annual_income").show()

# Using head() and take()
first_5 = df.head(5)  # Returns list of Row objects
print(f"First 5 rows: {len(first_5)} records")

# PERCENTILE RANKING
# Percent rank - relative rank as percentage
windowSpec2 = Window.orderBy(col("annual_income"))
df_percentile = df.withColumn("income_percentile", percent_rank().over(windowSpec2))

# Ntile - divide into N buckets
df_quartile = df.withColumn("income_quartile", ntile(4).over(windowSpec2))
df_decile = df.withColumn("income_decile", ntile(10).over(windowSpec2))

print("Percentile analysis:")
df_quartile.select("customer_id", "annual_income", "income_quartile") \
    .orderBy("annual_income").show(10)

# Lab: Customer and Transaction Ranking
print("=== Customer Ranking Lab ===")

# Comprehensive ranking analysis
window_state = Window.partitionBy("state").orderBy(col("annual_income").desc())
window_overall = Window.orderBy(col("annual_income").desc())

df_customer_ranking = df.withColumn(
    "state_rank", row_number().over(window_state)
).withColumn(
    "overall_rank", row_number().over(window_overall)
).withColumn(
    "income_percentile", round(percent_rank().over(window_overall) * 100, 2)
).withColumn(
    "top_10_percent", col("income_percentile") >= 90
)

# Get top 3 customers per state
top_per_state = df_customer_ranking.filter(col("state_rank") <= 3)

print("Top 3 customers per state:")
top_per_state.select(
    "state", "customer_id", "annual_income", 
    "state_rank", "overall_rank", "income_percentile"
).orderBy("state", "state_rank").show(20)

spark.stop()
```

---

## Chapter 9: Data Frequency, Counts and Unique Operations

### Frequency Analysis and Counting

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("FrequencyCounts").getOrCreate()

# Load data
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("banking_dataset/datasets/customers.csv")

# FREQUENCY ANALYSIS
# Value counts for categorical columns
segment_counts = df.groupBy("customer_segment") \
    .count() \
    .orderBy("count", ascending=False)

print("Customer segment frequency:")
segment_counts.show()

# Frequency with percentage
total_customers = df.count()
segment_freq = segment_counts.withColumn(
    "percentage", 
    round(col("count") / total_customers * 100, 2)
)
segment_freq.show()

# Multiple column frequency
state_segment_freq = df.groupBy("state", "customer_segment") \
    .count() \
    .orderBy("state", "count", ascending=False)

print("Frequency by state and segment:")
state_segment_freq.show(20)

# COUNTING OPERATIONS
# Basic count
total_count = df.count()
print(f"Total records: {total_count}")

# Count non-null values
email_count = df.filter(col("email").isNotNull()).count()
print(f"Customers with email: {email_count}")

# Count distinct values
distinct_states = df.select("state").distinct().count()
print(f"Number of states: {distinct_states}")

# countDistinct in aggregation
df_agg = df.groupBy("customer_segment").agg(
    count("*").alias("total_count"),
    countDistinct("state").alias("unique_states"),
    countDistinct("risk_score").alias("unique_risk_scores")
)
df_agg.show()

# Approximate count distinct (faster for large datasets)
approx_unique = df.select(approx_count_distinct("customer_id", 0.05)).collect()[0][0]
print(f"Approximate unique customers: {approx_unique}")

# UNIQUE VALUE HANDLING
# Get distinct rows
distinct_df = df.select("customer_segment", "state").distinct()
print(f"Unique segment-state combinations: {distinct_df.count()}")

# Drop duplicates based on specific columns
df_no_dup = df.dropDuplicates(["customer_id"])
print(f"After removing duplicates: {df_no_dup.count()}")

# Keep first/last occurrence
# Keep first occurrence of each customer_segment
df_first = df.dropDuplicates(["customer_segment"])
print(f"One record per segment (first): {df_first.count()}")

# Advanced duplicate handling
windowSpec = Window.partitionBy("customer_segment").orderBy("annual_income")
df_with_row = df.withColumn("row_num", row_number().over(windowSpec))

# Keep only the highest income customer per segment
df_highest = df_with_row.filter(col("row_num") == 1).drop("row_num")

# Lab: Data Quality and Frequency Analysis
print("=== Data Quality and Frequency Analysis Lab ===")

# Comprehensive data quality report
quality_report = spark.createDataFrame([
    ("Total Records", df.count()),
    ("Unique Customers", df.select("customer_id").distinct().count()),
    ("Duplicate Customer IDs", df.count() - df.select("customer_id").distinct().count()),
    ("Records with Email", df.filter(col("email").isNotNull()).count()),
    ("Records with Phone", df.filter(col("phone").isNotNull()).count()),
    ("Unique States", df.select("state").distinct().count()),
    ("Unique Segments", df.select("customer_segment").distinct().count()),
    ("High Risk Customers", df.filter(col("risk_score") < 600).count()),
    ("High Value Customers", df.filter(col("annual_income") > 100000).count())
], ["Metric", "Count"])

print("Data Quality Report:")
quality_report.show(truncate=False)

# Frequency distribution for risk scores
risk_distribution = df.select("risk_score") \
    .withColumn("risk_range",
        when(col("risk_score") < 500, "0-499")
        .when(col("risk_score") < 600, "500-599")
        .when(col("risk_score") < 700, "600-699")
        .when(col("risk_score") < 800, "700-799")
        .otherwise("800+")
    ) \
    .groupBy("risk_range") \
    .count() \
    .orderBy("risk_range")

print("Risk Score Distribution:")
risk_distribution.show()

spark.stop()
```

---

## Chapter 10: Creating Unique Identifiers

### Script 08: Indexing and Unique Identifiers

Creating unique IDs for tracking and joining data.

```python
#!/usr/bin/env python3
"""
Script 08: Indexing and Unique Identifiers
From PySpark Tutorial - Creating unique identifiers and indexing fundamentals
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    monotonically_increasing_id, concat, lit, year, month, lpad,
    md5, sha2, col
)

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Indexing_Unique_Identifiers") \
        .getOrCreate()

    print("=== Indexing Fundamentals - Creating Unique Identifiers ===")
    
    try:
        # Load our customers data
        customers_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("banking_dataset/datasets/customers.csv")

        print(f"Loaded {customers_df.count()} customer records")

        print("\n1. Creating Simple Sequential IDs")
        # Add a unique ID to each row
        customers_with_id = customers_df.withColumn(
            "row_id", 
            monotonically_increasing_id()
        )

        print("Added unique row IDs:")
        customers_with_id.select("row_id", "customer_id", "first_name").show(10)
        print("Note: monotonically_increasing_id() creates unique IDs but they're not sequential")
        print("They're guaranteed to be unique and increasing, but may have gaps")

        print("\n2. Creating Business Keys")
        # Create a composite key from multiple columns
        customers_with_key = customers_df.withColumn(
            "customer_key",
            concat(
                col("state"),
                lit("_"),
                col("customer_segment"),
                lit("_"),
                col("customer_id")
            )
        )

        print("Created composite business key:")
        customers_with_key.select("customer_key", "state", "customer_segment", "customer_id").show(5)

        print("\n3. Creating Time-Based Keys")
        # Create a year-month key for time-based analysis
        # First, add a date column for demonstration
        customers_with_date = customers_df.withColumn(
            "account_open_date",
            lit("2023-01-15").cast("date")
        )

        customers_with_period = customers_with_date.withColumn(
            "period_key",
            concat(
                year("account_open_date").cast("string"),
                lit("-"),
                lpad(month("account_open_date"), 2, "0")  # Pad month with zero
            )
        )

        print("Created period key:")
        customers_with_period.select("customer_id", "account_open_date", "period_key").show(5)

        print("\n4. Creating Hash-Based IDs")
        # Create an MD5 hash of customer information
        customers_hashed = customers_df.withColumn(
            "customer_hash",
            md5(concat(col("customer_id"), col("email")))
        )

        print("Created MD5 hash IDs:")
        customers_hashed.select("customer_id", "email", "customer_hash").show(5, truncate=False)

        print("\n5. Creating Secure Hash-Based IDs")
        # Use SHA-256 for more secure hashing
        customers_sha = customers_df.withColumn(
            "secure_hash",
            sha2(concat(col("customer_id"), col("email")), 256)
        )

        print("Created SHA-256 hash IDs:")
        customers_sha.select("customer_id", "secure_hash").show(3, truncate=False)

        print("\n6. Comprehensive Indexing Example")
        # Create a comprehensive customer index with multiple ID types
        customer_master_index = customers_df \
            .withColumn("row_id", monotonically_increasing_id()) \
            .withColumn(
                "business_key",
                concat(
                    col("state"),
                    lit("-"),
                    col("customer_segment"),
                    lit("-"),
                    col("customer_id")
                )
            ) \
            .withColumn(
                "secure_id",
                sha2(concat(col("customer_id"), col("email"), col("first_name")), 256)
            ) \
            .withColumn("bank_code", lit("BNK001"))

        print("Comprehensive customer index:")
        customer_master_index.select(
            "row_id", 
            "customer_id", 
            "business_key", 
            "bank_code",
            "secure_id"
        ).show(5, truncate=False)

        print("\n7. Index Usage Examples")
        print("Why we need indexes:")
        print("- Uniquely identify each row")
        print("- Join tables accurately")
        print("- Track specific records through transformations")
        print("- Debug data issues")
        print("- Security and privacy (hashed IDs)")

        print("\nIndex Types Summary:")
        print("- Sequential IDs: For simple row numbering")
        print("- Business Keys: Meaningful composite identifiers")
        print("- Hash IDs: For security or data anonymization")
        print("- Time-based Keys: For temporal analysis")

        print("\n8. Practical Example - Customer Lookup Table")
        # Create a lookup table that could be saved and used later
        lookup_table = customers_df.select(
            col("customer_id").alias("original_id"),
            monotonically_increasing_id().alias("internal_id"),
            concat(
                col("state"), lit("_"), 
                col("customer_segment"), lit("_"), 
                col("customer_id")
            ).alias("business_key"),
            md5(col("customer_id")).alias("anonymized_id")
        )

        print("Customer lookup table:")
        lookup_table.show(10)
        
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure the banking_dataset/datasets/customers.csv file exists")
    
    # Stop Spark session
    spark.stop()
    print("\nScript completed successfully!")

if __name__ == "__main__":
    main()
```

---

## Chapter 11: Complete Banking Analysis Lab

### Script 09: Comprehensive Banking Analysis

Putting everything together in a complete analysis.

```python
#!/usr/bin/env python3
"""
Script 09: Complete Banking Analysis Lab
From PySpark Tutorial - Comprehensive hands-on banking data analysis
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

def main():
    # Create Spark session for the banking analysis lab
    spark = SparkSession.builder \
        .appName("Banking_Analysis_Lab") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    print("=" * 60)
    print("BANKING ANALYSIS LAB: Comprehensive Customer Analysis")
    print("=" * 60)

    try:
        # Define data paths
        data_path = "banking_dataset/datasets/"
        
        print("\n=== STEP 1: LOADING BANKING DATA ===")
        
        # Define a proper schema for customers
        customer_schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("risk_score", IntegerType(), True),
            StructField("annual_income", DoubleType(), True),
            StructField("employment_status", StringType(), True)
        ])

        # Load the data
        customers = spark.read \
            .option("header", "true") \
            .schema(customer_schema) \
            .csv(f"{data_path}customers.csv")

        print(f"✓ Loaded {customers.count()} customer records")
        
        # Try to load accounts and products
        try:
            accounts = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(f"{data_path}accounts.csv")
            print(f"✓ Loaded {accounts.count()} account records")
            accounts_available = True
        except:
            print("! Accounts file not available - continuing with customer analysis only")
            accounts_available = False

        print("\nSample of raw customer data:")
        customers.show(5)

        print("\n=== STEP 2: DATA CLEANING AND QUALITY CHECK ===")
        
        # Check data quality
        print("Checking for data quality issues:")
        customers.select("customer_id", "first_name", "last_name").show(5, truncate=False)

        # Clean the data
        customers_clean = customers \
            .withColumn("first_name", initcap(trim(col("first_name")))) \
            .withColumn("last_name", initcap(trim(col("last_name")))) \
            .withColumn("email", lower(trim(col("email")))) \
            .withColumn("city", initcap(trim(col("city"))))

        print("\n✓ After cleaning:")
        customers_clean.select("customer_id", "first_name", "last_name", "email").show(5, truncate=False)

        # Check for missing values
        print("\n=== Missing Value Analysis ===")
        null_counts = customers_clean.select([
            count(col(c).isNull().cast("int")).alias(c) for c in customers_clean.columns
        ])
        
        print("Missing values per column:")
        null_counts.show()

        print("\n=== STEP 3: CREATE ENHANCED CUSTOMER DATASET ===")
        
        # Add meaningful derived columns
        customers_enhanced = customers_clean \
            .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name"))) \
            .withColumn("income_level",
                when(col("annual_income") < 30000, "Low")
                .when(col("annual_income") < 60000, "Medium")
                .when(col("annual_income") < 100000, "High")
                .otherwise("Very High")
            ) \
            .withColumn("risk_category",
                when(col("risk_score") < 600, "High Risk")
                .when(col("risk_score") < 700, "Medium Risk")
                .when(col("risk_score") < 800, "Low Risk")
                .otherwise("Very Low Risk")
            )

        print("✓ Enhanced customer data with categories:")
        customers_enhanced.select(
            "customer_id", 
            "full_name", 
            "annual_income", 
            "income_level",
            "risk_score",
            "risk_category"
        ).show(10)

        print("\n=== STEP 4: CUSTOMER SEGMENT ANALYSIS ===")
        print("-" * 50)

        # Count customers by segment
        segment_counts = customers_enhanced \
            .groupBy("customer_segment") \
            .count() \
            .orderBy("count", ascending=False)

        print("Customer distribution by segment:")
        segment_counts.show()

        # Detailed segment analysis
        segment_analysis = customers_enhanced \
            .groupBy("customer_segment") \
            .agg(
                count("*").alias("customer_count"),
                avg("annual_income").alias("avg_income"),
                min("annual_income").alias("min_income"),
                max("annual_income").alias("max_income"),
                avg("risk_score").alias("avg_risk_score")
            ) \
            .orderBy("avg_income", ascending=False)

        print("\nDetailed segment metrics:")
        segment_analysis.show()

        print("\n=== STEP 5: GEOGRAPHIC ANALYSIS ===")
        
        # State-wise customer distribution
        state_report = customers_enhanced \
            .groupBy("state") \
            .agg(
                count("*").alias("customer_count"),
                avg("annual_income").alias("avg_income"),
                avg("risk_score").alias("avg_risk_score")
            ) \
            .orderBy("customer_count", ascending=False)

        print("Top states by customer count:")
        state_report.show(10)

        print("\n=== STEP 6: CUSTOMER PROFILING ===")
        
        # High-value customers
        high_value_customers = customers_enhanced.filter(
            (col("annual_income") > 100000) & 
            (col("risk_score") > 750)
        )

        print(f"✓ Identified {high_value_customers.count()} high-value customers")
        print("Sample high-value customers:")
        high_value_customers.select(
            "customer_id",
            "full_name",
            "annual_income",
            "risk_score",
            "customer_segment"
        ).show(10)

        # Risk alert
        high_risk_customers = customers_enhanced.filter(col("risk_score") < 600)
        print(f"⚠ Risk Alert: {high_risk_customers.count()} high-risk customers identified")

        print("\n=== STEP 7: CREATE MASTER CUSTOMER INDEX ===")
        
        # Create a comprehensive customer index
        customer_index = customers_enhanced \
            .withColumn("index_id", monotonically_increasing_id()) \
            .withColumn(
                "customer_key",
                concat(
                    col("state"),
                    lit("-"),
                    col("customer_segment"),
                    lit("-"),
                    col("customer_id")
                )
            ) \
            .withColumn(
                "customer_hash", 
                md5(concat(col("customer_id"), col("email")))
            ) \
            .select(
                "index_id",
                "customer_id",
                "customer_key",
                "customer_hash",
                "full_name",
                "email",
                "state",
                "customer_segment",
                "income_level",
                "risk_category",
                "annual_income",
                "risk_score"
            )

        print("✓ Master Customer Index created:")
        customer_index.show(5)

        print("\n=== STEP 8: SAVE ANALYSIS RESULTS ===")
        
        # Create output directory
        os.makedirs("output", exist_ok=True)

        try:
            # Save segment analysis as CSV
            segment_analysis.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv("output/segment_analysis")

            print("✓ Saved segment analysis as CSV")

            # Save enhanced customers as Parquet
            customers_enhanced.write \
                .mode("overwrite") \
                .parquet("output/customers_enhanced")

            print("✓ Saved enhanced customers as Parquet")

            # Save customer index
            customer_index.write \
                .mode("overwrite") \
                .parquet("output/customer_index")

            print("✓ Saved customer index")

        except Exception as e:
            print(f"Save error: {e}")

        print("\n" + "="*60)
        print("LAB SUMMARY: Banking Data Analysis Complete")
        print("="*60)

        print(f"""
Data Processed:
- Total Customers: {customers.count():,}
- Customer Segments: {customers_enhanced.select("customer_segment").distinct().count()}
- States Covered: {customers_enhanced.select("state").distinct().count()}

Key Findings:
- High-value customers: {high_value_customers.count()}
- High-risk customers: {high_risk_customers.count()}
- Average income: ${customers_enhanced.select(avg("annual_income")).collect()[0][0]:,.2f}
- Average risk score: {customers_enhanced.select(avg("risk_score")).collect()[0][0]:.1f}

Files Created:
- output/customer_index/ (Parquet)
- output/segment_analysis/ (CSV)
- output/customers_enhanced/ (Parquet)

Skills Demonstrated:
✓ Loading data with explicit schemas
✓ Data cleaning and quality assessment
✓ Creating derived columns and categories
✓ Advanced filtering and grouping
✓ Statistical analysis and aggregation
✓ Creating unique identifiers and indexes
✓ Joining DataFrames (when accounts available)
✓ Saving data in multiple formats
✓ Comprehensive business analysis
""")

    except Exception as e:
        print(f"Error in banking analysis: {e}")
        print("Make sure the banking_dataset/datasets/customers.csv file exists")
    
    # Stop Spark session
    spark.stop()
    print("\n✓ Banking Analysis Lab completed successfully!")

if __name__ == "__main__":
    main()
```
---

## Chapter 12: Working with Cloud Storage (S3)

In real-world banking and financial systems, data rarely lives only on your laptop. Most institutions store customer records, transactions, and processed analytics securely in **cloud storage**. Amazon S3 (Simple Storage Service) is one of the most widely used platforms for this. With PySpark, you can directly **read and write huge datasets to S3** without downloading them locally. This means you can run analysis on terabytes of data stored in the cloud, scale out across multiple machines, and keep everything consistent with production data pipelines.

In this chapter, you’ll learn how to configure Spark to talk to S3, read Parquet files stored there, and write results back as Parquet or CSV. This is an essential step if you want to move from working with small demo files on your laptop to handling **enterprise-grade banking datasets** in the cloud.


### Script 10: S3 Parquet Reader

```python
#!/usr/bin/env python3
"""
Script 10: S3 Parquet Reader
From PySpark Tutorial - Reading parquet files directly from S3 using PySpark

This script:
1) Creates a Spark session configured for S3 access
2) Reads parquet files directly from an S3 path (recursively)
3) Displays sample rows, schema, and total record count
4) Stops the Spark session

Requirements:
- AWS credentials must be available (environment variables, aws configure, or IAM role)
- hadoop-aws and aws-java-sdk-bundle provided via Spark packages
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
        df = spark.read.parquet(s3_path)
        
        # Display sample data (default: 20 rows)
        print("Sample data from parquet files:")
        df.show()
        
        # Display schema information
        print("Data Schema:")
        df.printSchema()
        
        # Count total records
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
```

---

## Common Troubleshooting

* **Error: `No FileSystem for scheme "s3a"`**
  → Add Hadoop AWS connector:
  `--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262`

* **Error: Access Denied**
  → Verify AWS credentials and S3 bucket permissions.

* **Slow jobs / too many small files**
  → Use `.repartition()` before writing, and partition on low-cardinality columns (e.g., state).

* **Overwriting partitions fails**
  → Set `spark.sql.sources.partitionOverwriteMode=dynamic`.

* **CSV schema issues**
  → Prefer Parquet, or define explicit schema for CSV.

---

## Best Practices Summary

### Schema Management
1. **Always define explicit schemas** for production data
2. **Verify data types** match your business logic (e.g., ZIP codes as strings)
3. **Document nullable fields** in your schema definition

### DataFrame Operations
1. **Use column objects** instead of strings for better type safety
2. **Filter early** to reduce data volume before expensive operations
3. **Select only needed columns** to improve performance

### Data Quality
1. **Check for nulls** before operations that don't handle them
2. **Remove duplicates** based on business keys
3. **Validate data types** after loading

### Indexing
1. **Create unique identifiers** early in your pipeline
2. **Use business keys** that are meaningful to your domain
3. **Consider hash-based IDs** for distributed operations

### Performance Tips
1. **Use Parquet format** for intermediate storage
2. **Avoid collect()** on large DataFrames
3. **Cache DataFrames** that are used multiple times

---

## Common Patterns and Examples

### Pattern 1: Load → Clean → Transform → Save
```python
# Load
df = spark.read.schema(schema).csv("input.csv")

# Clean
df_clean = df.dropna().dropDuplicates()

# Transform
df_transformed = df_clean.withColumn("new_col", expression)

# Save
df_transformed.write.parquet("output.parquet")
```

### Pattern 2: Multi-Source Join
```python
# Load multiple sources
customers = spark.read.parquet("customers.parquet")
accounts = spark.read.parquet("accounts.parquet")
transactions = spark.read.parquet("transactions.parquet")

# Join step by step
customer_accounts = customers.join(accounts, "customer_id", "left")
full_data = customer_accounts.join(transactions, "account_id", "left")
```

### Pattern 3: Aggregation Pipeline
```python
# Read → Filter → Group → Aggregate → Sort
result = df.filter(condition) \
    .groupBy("category") \
    .agg(count("*").alias("count")) \
    .orderBy("count", ascending=False)
```

---

## Quick Reference Card

```python
# Essential DataFrame Operations

# Create SparkSession
spark = SparkSession.builder.appName("app_name").getOrCreate()

# Read Data
df = spark.read.csv("file.csv")                          # Basic
df = spark.read.option("header","true").csv("file.csv")  # With header
df = spark.read.schema(schema).csv("file.csv")           # With schema
df = spark.read.parquet("file.parquet")                  # Parquet
df = spark.read.json("file.json")                        # JSON

# Inspect
df.show(n)                  # Display n rows
df.printSchema()            # Show structure  
df.count()                  # Row count
df.columns                  # Column names
df.describe().show()        # Statistics

# Select & Filter
df.select("col1", "col2")              # Select columns
df.filter(col("age") > 21)             # Filter rows
df.where("state = 'NY'")                # SQL-style filter

# Transform
df.withColumn("new", expression)        # Add column
df.drop("column")                       # Remove column
df.withColumnRenamed("old", "new")      # Rename

# Handle Nulls
df.dropna()                             # Remove null rows
df.fillna(value)                        # Fill nulls

# Aggregate
df.groupBy("col").count()               # Count by group
df.groupBy("col").agg(avg("value"))     # Average by group

# Join
df1.join(df2, "key")                    # Inner join
df1.join(df2, "key", "left")            # Left join

# Index
df.withColumn("id", monotonically_increasing_id())  # Add unique ID

# Save
df.write.mode("overwrite").parquet("output")        # Parquet
df.write.option("header","true").csv("output")      # CSV
```

---

## Conclusion

You've now learned the fundamentals of PySpark DataFrames and Schema Management:
- Creating DataFrames from various sources
- Defining and managing schemas
- Performing basic operations and exploration
- Creating indexes for data tracking
- Working with real banking data

These skills form the foundation for all Spark data processing. Practice with different datasets to build confidence before moving to more advanced topics like string manipulation, conditionals, and window functions in the next sections of the course.