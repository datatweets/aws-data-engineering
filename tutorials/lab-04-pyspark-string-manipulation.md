# PySpark String/Character Manipulation Tutorial

## String and Character Manipulation (Core Functions)

String manipulation is one of the most common tasks in data processing. In banking and business data, you'll frequently encounter inconsistent text data that needs cleaning and standardization. PySpark provides powerful functions to handle string operations at scale.

### Prerequisites

Before starting this chapter, ensure you have:

- PySpark installed and configured
- Access to the banking dataset from previous chapters
- Basic understanding of DataFrame operations

------

## String Trimming Functions: Data Cleaning Essentials

Raw data often contains unwanted whitespace that can cause issues in analysis and joins. PySpark provides three essential trimming functions.

### String Trimming Operations

```python
#!/usr/bin/env python3
"""
Script 12: String Trimming Functions
From PySpark Tutorial - Cleaning whitespace from string data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, ltrim, rtrim, col, length

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("String_Trimming_Operations") \
        .getOrCreate()

    print("=== String Trimming Functions ===")
    
    # Create sample data with whitespace issues (common in real banking data)
    sample_data = [
        ("CUST001", "  John  ", "Smith   ", "  john.smith@email.com"),
        ("CUST002", " Sarah ", "  Johnson", "sarah.j@bank.com  "),
        ("CUST003", "Mike   ", " Wilson ", "   mike.wilson@company.org  "),
        ("CUST004", "  Lisa", "Brown  ", "lisa.brown@service.net   ")
    ]
    
    columns = ["customer_id", "first_name", "last_name", "email"]
    df = spark.createDataFrame(sample_data, columns)

    print("Original data with whitespace (notice the spacing):")
    df.select("first_name", "last_name", "email").show(truncate=False)
    
    # Show string lengths to demonstrate whitespace
    print("String lengths before trimming:")
    df.select(
        "first_name",
        length("first_name").alias("fname_length"),
        "last_name", 
        length("last_name").alias("lname_length"),
        "email",
        length("email").alias("email_length")
    ).show()

    print("\n1. trim() - Remove leading AND trailing spaces")
    df_trimmed = df.withColumn("first_name_trimmed", trim(col("first_name"))) \
                   .withColumn("last_name_trimmed", trim(col("last_name"))) \
                   .withColumn("email_trimmed", trim(col("email")))

    print("After trim():")
    df_trimmed.select("first_name_trimmed", "last_name_trimmed", "email_trimmed").show(truncate=False)

    print("\n2. ltrim() - Remove only LEADING (left) spaces")
    df_ltrimmed = df.withColumn("first_name_ltrim", ltrim(col("first_name")))
    
    print("Comparison - original vs ltrim():")
    df_ltrimmed.select(
        col("first_name").alias("original"),
        col("first_name_ltrim").alias("left_trimmed"),
        length("first_name").alias("orig_len"),
        length("first_name_ltrim").alias("ltrim_len")
    ).show(truncate=False)

    print("\n3. rtrim() - Remove only TRAILING (right) spaces")
    df_rtrimmed = df.withColumn("email_rtrim", rtrim(col("email")))
    
    print("Comparison - original vs rtrim():")
    df_rtrimmed.select(
        col("email").alias("original"),
        col("email_rtrim").alias("right_trimmed"),
        length("email").alias("orig_len"),
        length("email_rtrim").alias("rtrim_len")
    ).show(truncate=False)

    print("\n4. Complete cleaning pipeline")
    df_clean = df.withColumn("first_name", trim(col("first_name"))) \
                 .withColumn("last_name", trim(col("last_name"))) \
                 .withColumn("email", trim(col("email")))

    print("Final cleaned data:")
    df_clean.select("customer_id", "first_name", "last_name", "email").show(truncate=False)

    print("\n✓ Key Takeaways:")
    print("- trim(): Most commonly used - removes both leading and trailing spaces")
    print("- ltrim(): Removes only leading spaces")
    print("- rtrim(): Removes only trailing spaces")
    print("- Always trim user input data before storing or joining")

    spark.stop()
    print("\nScript completed successfully!")

if __name__ == "__main__":
    main()
```

------

## String Replacement and Removal

Clean data often requires removing or replacing specific characters or patterns. PySpark offers multiple approaches for different scenarios.

### String Replacement Operations

```python
#!/usr/bin/env python3
"""
Script 13: String Replacement and Removal
From PySpark Tutorial - Replacing and removing characters from strings
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, translate, col

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("String_Replacement_Operations") \
        .getOrCreate()

    print("=== String Replacement and Removal ===")
    
    # Sample banking data with formatting issues
    sample_data = [
        ("CUST001", "(555) 123-4567", "123-45-6789", "John's Account"),
        ("CUST002", "555.987.6543", "987-65-4321", "Sarah & Mike's Joint"),
        ("CUST003", "555 456 7890", "456.78.9012", "Business Acct #1"),
        ("CUST004", "+1-555-234-5678", "234-56-7890", "Personal Account")
    ]
    
    columns = ["customer_id", "phone", "ssn", "account_name"]
    df = spark.createDataFrame(sample_data, columns)

    print("Original data with formatting inconsistencies:")
    df.show(truncate=False)

    print("\n1. regexp_replace() - Pattern-based replacement")
    print("Remove all non-numeric characters from phone numbers:")
    
    df_phone_clean = df.withColumn(
        "phone_clean",
        regexp_replace(col("phone"), "[^0-9]", "")  # Keep only digits
    )
    
    df_phone_clean.select("phone", "phone_clean").show(truncate=False)

    print("\n2. Standardize phone number format")
    # First clean, then format
    df_phone_formatted = df_phone_clean.withColumn(
        "phone_formatted",
        regexp_replace(col("phone_clean"), "(\\d{3})(\\d{3})(\\d{4})", "($1) $2-$3")
    )
    
    df_phone_formatted.select("phone", "phone_clean", "phone_formatted").show(truncate=False)

    print("\n3. translate() - Character-by-character replacement")
    print("Remove dots and dashes from SSN:")
    
    df_ssn_clean = df.withColumn(
        "ssn_clean",
        translate(col("ssn"), ".-", "")  # Remove dots and dashes
    )
    
    df_ssn_clean.select("ssn", "ssn_clean").show(truncate=False)

    print("\n4. Multiple replacements with chaining")
    print("Clean account names - remove special characters:")
    
    df_account_clean = df.withColumn("account_name_clean",
        regexp_replace(
            regexp_replace(
                regexp_replace(col("account_name"), "'", ""),  # Remove apostrophes
                "&", "and"  # Replace & with "and"
            ),
            "#", "Number "  # Replace # with "Number "
        )
    )
    
    df_account_clean.select("account_name", "account_name_clean").show(truncate=False)

    print("\n5. Advanced pattern replacement")
    print("Extract area code from phone numbers:")
    
    df_area_code = df_phone_clean.withColumn(
        "area_code",
        regexp_replace(col("phone_clean"), "(\\d{3})(\\d{7})", "$1")
    )
    
    df_area_code.select("phone", "phone_clean", "area_code").show(truncate=False)

    print("\n6. Complete data standardization")
    df_standardized = df \
        .withColumn("phone_standardized", 
            regexp_replace(
                regexp_replace(col("phone"), "[^0-9]", ""), 
                "(\\d{3})(\\d{3})(\\d{4})", 
                "($1) $2-$3"
            )
        ) \
        .withColumn("ssn_standardized",
            translate(col("ssn"), ".-", "")
        ) \
        .withColumn("account_name_standardized",
            regexp_replace(
                regexp_replace(col("account_name"), "'", ""),
                "&", "and"
            )
        )

    print("Final standardized data:")
    df_standardized.select(
        "customer_id", 
        "phone_standardized", 
        "ssn_standardized", 
        "account_name_standardized"
    ).show(truncate=False)

    print("\n✓ Key Functions:")
    print("- regexp_replace(): Use for complex patterns and formatting")
    print("- translate(): Use for simple character-by-character replacement")
    print("- Chain multiple replacements for complex cleaning")

    spark.stop()
    print("\nScript completed successfully!")

if __name__ == "__main__":
    main()
```

------

## String Concatenation

Combining text fields is essential for creating full names, addresses, and composite keys.

### String Concatenation Operations

```python
#!/usr/bin/env python3
"""
Script 14: String Concatenation
From PySpark Tutorial - Combining strings and formatting text
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, concat_ws, format_string, col, lit

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("String_Concatenation_Operations") \
        .getOrCreate()

    print("=== String Concatenation Operations ===")
    
    # Sample customer data
    sample_data = [
        ("CUST001", "John", "Smith", "123 Main St", "Anytown", "NY", "12345", 75000),
        ("CUST002", "Sarah", "Johnson", "456 Oak Ave", "Springfield", "CA", "90210", 85000),
        ("CUST003", "Mike", "Wilson", "789 Pine Rd", "Riverside", "TX", "73301", 65000),
        ("CUST004", "Lisa", "Brown", "321 Elm St", "Madison", "FL", "32608", 95000)
    ]
    
    columns = ["customer_id", "first_name", "last_name", "street", "city", "state", "zip_code", "salary"]
    df = spark.createDataFrame(sample_data, columns)

    print("Original customer data:")
    df.show()

    print("\n1. concat() - Basic string concatenation")
    print("Create full name (without separator):")
    
    df_concat = df.withColumn("full_name_basic", concat(col("first_name"), col("last_name")))
    df_concat.select("first_name", "last_name", "full_name_basic").show()

    print("\n2. concat() with literal separators")
    print("Create full name with space separator:")
    
    df_with_space = df.withColumn(
        "full_name", 
        concat(col("first_name"), lit(" "), col("last_name"))
    )
    df_with_space.select("first_name", "last_name", "full_name").show()

    print("\n3. concat_ws() - Concatenate with separator (recommended)")
    print("Create full name using concat_ws:")
    
    df_concat_ws = df.withColumn("full_name_ws", concat_ws(" ", col("first_name"), col("last_name")))
    df_concat_ws.select("first_name", "last_name", "full_name_ws").show()

    print("\n4. Creating full addresses")
    print("Combine address components:")
    
    df_address = df.withColumn(
        "full_address",
        concat_ws(", ", col("street"), col("city"), col("state"), col("zip_code"))
    )
    df_address.select("street", "city", "state", "zip_code", "full_address").show(truncate=False)

    print("\n5. format_string() - Template-based formatting")
    print("Create formatted customer summary:")
    
    df_formatted = df.withColumn(
        "customer_summary",
        format_string(
            "%s %s from %s, %s (Salary: $%,.2f)",
            col("first_name"), 
            col("last_name"), 
            col("city"), 
            col("state"), 
            col("salary")
        )
    )
    df_formatted.select("customer_summary").show(truncate=False)

    print("\n6. Creating business keys")
    print("Create composite customer keys:")
    
    df_keys = df.withColumn(
        "customer_key",
        concat_ws("-", col("state"), col("customer_id"))
    ).withColumn(
        "location_key", 
        concat_ws("_", col("city"), col("state"), col("zip_code"))
    )
    
    df_keys.select("customer_id", "customer_key", "location_key").show()

    print("\n7. Advanced concatenation with conditionals")
    print("Create customer display name with title:")
    
    from pyspark.sql.functions import when
    
    df_display = df.withColumn("salary_level",
        when(col("salary") >= 90000, "Executive")
        .when(col("salary") >= 70000, "Senior")
        .otherwise("Standard")
    ).withColumn("display_name",
        concat_ws(" ", 
            col("salary_level"),
            col("first_name"),
            col("last_name")
        )
    )
    
    df_display.select("first_name", "last_name", "salary", "salary_level", "display_name").show()

    print("\n8. Handling null values in concatenation")
    # Add some null data to demonstrate
    null_data = [("CUST005", "Anna", None, "999 Test St", "TestCity", "CA", "99999", 70000)]
    df_with_null = df.union(spark.createDataFrame(null_data, columns))
    
    print("Data with null last name:")
    df_with_null.filter(col("customer_id") == "CUST005").show()
    
    # concat_ws handles nulls gracefully
    df_null_safe = df_with_null.withColumn(
        "safe_full_name",
        concat_ws(" ", col("first_name"), col("last_name"))
    )
    
    print("concat_ws handles nulls (skips null values):")
    df_null_safe.filter(col("customer_id") == "CUST005").select("first_name", "last_name", "safe_full_name").show()

    print("\n✓ Best Practices:")
    print("- Use concat_ws() instead of concat() for better null handling")
    print("- format_string() is great for complex formatting with placeholders")
    print("- Always consider null values when concatenating")
    print("- Create meaningful business keys using concatenation")

    spark.stop()
    print("\nScript completed successfully!")

if __name__ == "__main__":
    main()
```

------

## String Formatting and Case Operations

Consistent text formatting is crucial for data quality and user presentation.

### String Formatting Operations

```python
#!/usr/bin/env python3
"""
Script 15: String Formatting and Case Operations
From PySpark Tutorial - Standardizing text case and formatting
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, lower, initcap, col

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("String_Formatting_Operations") \
        .getOrCreate()

    print("=== String Formatting and Case Operations ===")
    
    # Sample data with inconsistent case formatting (common in real data)
    sample_data = [
        ("CUST001", "john smith", "ACCOUNTING@COMPANY.COM", "new york", "PREMIUM"),
        ("CUST002", "Sarah JOHNSON", "Sarah.Johnson@Email.Com", "los angeles", "standard"),
        ("CUST003", "MIKE wilson", "mike.WILSON@service.NET", "CHICAGO", "Premium"),
        ("CUST004", "lisa Brown", "Lisa.Brown@BANK.org", "miami", "STANDARD")
    ]
    
    columns = ["customer_id", "full_name", "email", "city", "segment"]
    df = spark.createDataFrame(sample_data, columns)

    print("Original data with inconsistent case:")
    df.show(truncate=False)

    print("\n1. upper() - Convert to UPPERCASE")
    print("Standardize customer segment to uppercase:")
    
    df_upper = df.withColumn("segment_upper", upper(col("segment")))
    df_upper.select("segment", "segment_upper").show()

    print("\n2. lower() - Convert to lowercase")
    print("Standardize email addresses to lowercase:")
    
    df_lower = df.withColumn("email_lower", lower(col("email")))
    df_lower.select("email", "email_lower").show(truncate=False)

    print("\n3. initcap() - Convert to Title Case")
    print("Standardize names and cities to proper case:")
    
    df_initcap = df.withColumn("full_name_proper", initcap(col("full_name"))) \
                   .withColumn("city_proper", initcap(col("city")))
    
    df_initcap.select("full_name", "full_name_proper", "city", "city_proper").show()

    print("\n4. Complete standardization pipeline")
    print("Apply consistent formatting across all text fields:")
    
    df_standardized = df \
        .withColumn("full_name_clean", initcap(col("full_name"))) \
        .withColumn("email_clean", lower(col("email"))) \
        .withColumn("city_clean", initcap(col("city"))) \
        .withColumn("segment_clean", upper(col("segment")))

    print("Standardized data:")
    df_standardized.select(
        "customer_id", 
        "full_name_clean", 
        "email_clean", 
        "city_clean", 
        "segment_clean"
    ).show(truncate=False)

    print("\n5. Case formatting for business rules")
    print("Apply different case rules for different data types:")
    
    # Example: Database fields uppercase, display fields title case, emails lowercase
    df_business_rules = df \
        .withColumn("database_segment", upper(col("segment"))) \
        .withColumn("display_name", initcap(col("full_name"))) \
        .withColumn("contact_email", lower(col("email"))) \
        .withColumn("location_display", initcap(col("city")))

    print("Business rule formatting:")
    df_business_rules.select(
        "database_segment", 
        "display_name", 
        "contact_email", 
        "location_display"
    ).show(truncate=False)

    print("\n6. Conditional case formatting")
    from pyspark.sql.functions import when
    
    # Different formatting based on segment type
    df_conditional = df.withColumn("formatted_segment",
        when(col("segment").isin(["premium", "Premium", "PREMIUM"]), "VIP Customer")
        .otherwise(initcap(col("segment")))
    )
    
    df_conditional.select("segment", "formatted_segment").show()

    print("\n✓ Formatting Standards:")
    print("- Names and addresses: initcap() for proper case")
    print("- Email addresses: lower() for consistency")
    print("- Database codes: upper() for standardization")
    print("- User display: initcap() for readability")

    spark.stop()
    print("\nScript completed successfully!")

if __name__ == "__main__":
    main()
```

------

## Advanced String Operations

These functions help extract, analyze, and manipulate specific parts of strings.

### Advanced String Operations

```python
#!/usr/bin/env python3
"""
Script 16: Advanced String Operations
From PySpark Tutorial - Substring, split, length, and padding operations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, split, length, lpad, rpad, col

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Advanced_String_Operations") \
        .getOrCreate()

    print("=== Advanced String Operations ===")
    
    # Sample data for advanced string operations
    sample_data = [
        ("CUST001", "John Michael Smith", "john.smith@email.com", "5551234567", "A123456789"),
        ("CUST002", "Sarah Jane Johnson", "s.johnson@company.net", "5559876543", "B987654321"),
        ("CUST003", "Michael Wilson", "mike.wilson@service.org", "5554567890", "C456789012"),
        ("CUST004", "Lisa Marie Brown", "l.brown@bank.gov", "5552345678", "D234567890")
    ]
    
    columns = ["customer_id", "full_name", "email", "phone", "account_number"]
    df = spark.createDataFrame(sample_data, columns)

    print("Original data:")
    df.show(truncate=False)

    print("\n1. substring() - Extract parts of strings")
    print("Extract first name (first word) and account prefix:")
    
    df_substring = df \
        .withColumn("first_initial", substring(col("full_name"), 1, 1)) \
        .withColumn("area_code", substring(col("phone"), 1, 3)) \
        .withColumn("account_prefix", substring(col("account_number"), 1, 1))
    
    df_substring.select("full_name", "first_initial", "phone", "area_code", "account_number", "account_prefix").show()

    print("\n2. split() - Split strings into arrays")
    print("Split full name into parts and email into username/domain:")
    
    df_split = df \
        .withColumn("name_parts", split(col("full_name"), " ")) \
        .withColumn("email_parts", split(col("email"), "@"))
    
    df_split.select("full_name", "name_parts", "email", "email_parts").show(truncate=False)

    print("\n3. Extract specific array elements after split")
    print("Get first name, last name, username, and domain:")
    
    df_extracted = df_split \
        .withColumn("first_name", col("name_parts")[0]) \
        .withColumn("last_name", col("name_parts")[2]) \
        .withColumn("username", col("email_parts")[0]) \
        .withColumn("domain", col("email_parts")[1])
    
    df_extracted.select("first_name", "last_name", "username", "domain").show()

    print("\n4. length() - String length analysis")
    print("Analyze string lengths for validation:")
    
    df_lengths = df \
        .withColumn("name_length", length(col("full_name"))) \
        .withColumn("email_length", length(col("email"))) \
        .withColumn("phone_length", length(col("phone")))
    
    df_lengths.select("full_name", "name_length", "email", "email_length", "phone", "phone_length").show()

    print("\n5. lpad() and rpad() - String padding")
    print("Pad account numbers and format customer IDs:")
    
    df_padded = df \
        .withColumn("account_padded", lpad(col("account_number"), 15, "0")) \
        .withColumn("customer_formatted", rpad(col("customer_id"), 10, "X"))
    
    df_padded.select("account_number", "account_padded", "customer_id", "customer_formatted").show(truncate=False)

    print("\n6. Complex string manipulation - Creating formatted IDs")
    print("Create standardized customer codes:")
    
    from pyspark.sql.functions import concat_ws, upper
    
    df_formatted_ids = df_split \
        .withColumn("customer_code",
            concat_ws("-",
                upper(substring(col("name_parts")[0], 1, 3)),  # First 3 letters of first name
                upper(substring(col("name_parts")[2], 1, 3)),  # First 3 letters of last name
                substring(col("phone"), 8, 4)  # Last 4 digits of phone
            )
        )
    
    df_formatted_ids.select("full_name", "phone", "customer_code").show()

    print("\n7. Data validation using string operations")
    print("Validate data quality with string functions:")
    
    from pyspark.sql.functions import when
    
    df_validation = df \
        .withColumn("name_valid", length(col("full_name")) >= 3) \
        .withColumn("email_valid", col("email").contains("@")) \
        .withColumn("phone_valid", length(col("phone")) == 10) \
        .withColumn("account_valid", length(col("account_number")) == 10)
    
    df_validation.select("customer_id", "name_valid", "email_valid", "phone_valid", "account_valid").show()

    print("\n8. Advanced pattern extraction")
    print("Extract middle names and create initials:")
    
    df_advanced = df_split \
        .withColumn("has_middle_name", 
            when(col("name_parts").getItem(1).isNotNull(), True).otherwise(False)
        ) \
        .withColumn("middle_initial",
            when(col("name_parts").getItem(1).isNotNull(), 
                substring(col("name_parts")[1], 1, 1)).otherwise("")
        ) \
        .withColumn("initials",
            concat_ws(".",
                substring(col("name_parts")[0], 1, 1),
                when(col("name_parts").getItem(1).isNotNull(), 
                    substring(col("name_parts")[1], 1, 1)).otherwise(""),
                substring(col("name_parts")[2], 1, 1)
            )
        )
    
    df_advanced.select("full_name", "has_middle_name", "middle_initial", "initials").show()

    print("\n✓ Advanced Operations Summary:")
    print("- substring(): Extract specific character ranges")
    print("- split(): Convert strings to arrays for complex parsing")
    print("- length(): Validate and analyze string sizes")
    print("- lpad()/rpad(): Format strings to consistent lengths")
    print("- Combine operations for complex data transformations")

    spark.stop()
    print("\nScript completed successfully!")

if __name__ == "__main__":
    main()
```

------

## 6.6 Assignment: Customer Data Standardization Lab

### Assignment Overview

You will apply all string manipulation techniques learned in this chapter to clean and standardize a realistic banking dataset. This assignment simulates real-world data integration scenarios where customer data arrives from multiple sources with inconsistent formatting.

### Learning Objectives

By completing this assignment, you will demonstrate mastery of:

- String trimming and whitespace handling
- Case standardization for different data types
- Pattern-based replacements using regular expressions
- String concatenation for creating business keys
- Advanced string parsing and extraction
- Data validation using string functions

------

### Assignment Setup

Create a new Python script named `customer_standardization_assignment.py` and implement the following requirements:

### Dataset

Use this messy banking customer data (copy into your script):

```python
messy_data = [
    ("CUST001", "  john michael SMITH  ", "JOHN.SMITH@email.COM  ", "(555) 123-4567", "  123 Main St  ", "new york", "ny", " 10001 ", "PREMIUM "),
    ("CUST002", "Sarah   Johnson", "s.johnson@Company.NET", "555.987.6543", "456 Oak Avenue", "LOS ANGELES", "CA", "90210", "standard"),
    ("CUST003", "MIKE WILSON", "  mike.wilson@SERVICE.org", "555 456 7890", "789 Pine Road ", "chicago", "IL", "60601 ", "Premium"),
    ("CUST004", "lisa marie Brown  ", "Lisa.Brown@BANK.gov  ", "+1-555-234-5678", "321 Elm Street", "MIAMI", "fl", "33101", " Standard "),
    ("CUST005", "robert  DAVIS", "r.davis@email.com", "5552345678", "654 Cedar Lane", "houston", "TX", "77001", "PREMIUM"),
    ("CUST006", "JENNIFER WHITE", "jennifer@company.net", "555-345-6789", "987 Birch Drive", "PHOENIX", "az", "85001", "standard")
]

schema_columns = ["customer_id", "full_name", "email", "phone", "address", "city", "state", "zip_code", "segment"]
```

------

### Required Tasks

#### Task 1: Environment Setup (5 points)

- Create a Spark session with appropriate configuration
- Load the messy data into a DataFrame with proper schema
- Display the raw data to understand quality issues

#### Task 2: Basic Data Cleaning (15 points)

- **Trim whitespace** from all string columns (except customer_id)
- Show before and after comparison
- Calculate and display string lengths to verify cleaning

#### Task 3: Case Standardization (15 points)

Apply appropriate case formatting:

- **Names and addresses**: Title case (initcap)
- **Email addresses**: Lowercase
- **State codes**: Uppercase
- **Customer segments**: Uppercase

#### Task 4: Phone Number Standardization (20 points)

- Remove all non-numeric characters from phone numbers
- Create a standardized format: `(555) 123-4567`
- Display original, cleaned digits, and formatted versions

#### Task 5: Email Processing (15 points)

- Validate emails contain "@" symbol
- Extract username and domain parts separately
- Convert all emails to lowercase

#### Task 6: Name Parsing (20 points)

- Split full names into components using `split()`
- Extract: first_name, middle_name (if exists), last_name
- Create initials field (e.g., "J.M.S." or "S.J." for no middle name)
- Handle cases with and without middle names

#### Task 7: Business Key Creation (20 points)

Create the following identifier fields:

- **customer_key**: Format as "STATE-SEGMENT-CUSTOMERID" (e.g., "NY-PREMIUM-CUST001")
- **location_key**: Format as "CITY_STATE_ZIPCODE" (remove spaces from city)
- **customer_code**: First 2 letters of first name + first 2 letters of last name + last 4 digits of phone
- **secure_hash**: MD5 hash of concatenated customer_id, email, and full_name

#### Task 8: Data Validation (15 points)

Create validation columns that return True/False:

- **name_length_valid**: Full name has at least 3 characters
- **email_format_valid**: Email matches basic email pattern
- **phone_length_valid**: Phone number has exactly 10 digits
- **state_code_valid**: State code is exactly 2 characters
- **zip_code_valid**: ZIP code is exactly 5 digits
- **segment_valid**: Segment is either "PREMIUM" or "STANDARD"

#### Task 9: Quality Scoring (10 points)

- Create a **quality_score** by summing all validation boolean fields (cast to int)
- Create a **quality_grade**:
  - 6 points = "A"
  - 5 points = "B"
  - 4 points = "C"
  - Less than 4 = "F"

#### Task 10: Final Dataset and Analysis (15 points)

- Create a final clean dataset with all standardized fields
- Generate summary reports:
  - Total customers processed
  - Quality grade distribution
  - Customer count by state
  - Customer count by segment

------

### Submission Requirements

#### Code Requirements

- Use proper PySpark syntax and functions
- Include comments explaining each transformation step
- Handle the Spark session properly (create and stop)
- Use meaningful variable names

#### Output Requirements

Your script must produce:

1. **Before/after data comparison** showing the cleaning impact
2. **Step-by-step transformations** with intermediate results displayed
3. **Final standardized dataset**
4. **Summary statistics** and quality report
5. **All required fields** from Tasks 1-10

#### Documentation

Include a brief comment block at the top explaining:

- Your approach to handling missing middle names
- Why you chose specific formatting standards
- Any assumptions made about the data

------

### Hints and Tips

1. **Start small**: Test each transformation on a few rows first
2. **Use show(truncate=False)**: To see full text values during development
3. **Check for nulls**: The `concat_ws()` function handles nulls better than `concat()`
4. **Regular expressions**: Use `[^0-9]` to match any non-digit character
5. **Array indexing**: Use `.getItem(0)` or `[0]` to access split array elements
6. **Conditional logic**: Use `when().otherwise()` for handling different cases

### Expected Output Format

Your final output should include clear section headers and show progression through each step:

```
=== RAW DATA ANALYSIS ===
[Display original messy data]

=== STEP 1: TRIMMING WHITESPACE ===
[Show before/after trimming]

=== STEP 2: CASE STANDARDIZATION ===
[Show case formatting results]

... [continue for each step] ...

=== FINAL STANDARDIZED DATASET ===
[Display clean, final dataset]

=== SUMMARY REPORT ===
Total customers: 6
Quality distribution: [A: 4, B: 2, C: 0, F: 0]
```

### Submission

Submit your completed Python script file. The script should run independently and produce all required outputs when executed.

------

## Chapter Summary

This chapter covered comprehensive string manipulation techniques essential for data cleaning and standardization in PySpark:

### Key Functions Mastered

**Trimming Functions:**

- `trim()`: Remove leading and trailing whitespace
- `ltrim()`: Remove only leading whitespace
- `rtrim()`: Remove only trailing whitespace

**Replacement Operations:**

- `regexp_replace()`: Pattern-based replacement using regular expressions
- `translate()`: Character-by-character replacement

**Concatenation:**

- `concat()`: Basic string concatenation
- `concat_ws()`: Concatenation with separator (handles nulls gracefully)
- `format_string()`: Template-based formatting with placeholders

**Case Formatting:**

- `upper()`: Convert to uppercase
- `lower()`: Convert to lowercase
- `initcap()`: Convert to title case

**Advanced Operations:**

- `substring()`: Extract portions of strings
- `split()`: Convert strings to arrays
- `length()`: Get string length
- `lpad()`/`rpad()`: Pad strings to specific lengths

### Real-World Applications

The lab demonstrated practical applications including:

- Phone number standardization and formatting
- Email address validation and cleaning
- Name parsing and initial extraction
- Address standardization
- Business key generation
- Data quality scoring

### Best Practices

1. **Always trim data** from external sources
2. **Use concat_ws()** instead of concat() for null safety
3. **Apply consistent case formatting** based on data type
4. **Validate data quality** using string functions
5. **Create meaningful business keys** using concatenation
6. **Document your standardization rules** for consistency

These string manipulation skills are fundamental for preparing clean, consistent datasets that are ready for analysis, reporting, and integration with other systems.