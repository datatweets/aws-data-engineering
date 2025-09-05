# PySpark Local Installation Guide

This guide will help you install PySpark locally on Windows and macOS systems.

## Prerequisites

PySpark requires Java 11, or 17. We recommend Java 11 for best compatibility.

---

## macOS Installation

### Step 1: Install Java (if not already installed)

#### Option A: Using Homebrew (Recommended)
```bash
# Install Homebrew if you don't have it
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Java 11
brew install openjdk@11

# Set JAVA_HOME in your shell profile
echo 'export JAVA_HOME="/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"' >> ~/.zshrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc

# Reload your shell configuration
source ~/.zshrc
```

#### Option B: Manual Installation
1. Download OpenJDK 11 from [Eclipse Temurin](https://adoptium.net/)
2. Install the downloaded `.pkg` file
3. Set JAVA_HOME:
```bash
echo 'export JAVA_HOME="/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home"' >> ~/.zshrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### Step 2: Verify Java Installation
```bash
java -version
echo $JAVA_HOME
```

### Step 3: Install Python and Create Virtual Environment
```bash
# Install Python using Homebrew
brew install python

# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate
```

### Step 4: Install PySpark
```bash
# Install PySpark
pip install pyspark==3.5.1

# Install additional dependencies for S3 support
pip install boto3 pandas pyarrow
```

### Step 5: Test Installation
```bash
python -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')"
```

---

## Windows Installation

### Step 1: Install Java

#### Option A: Using Chocolatey (Recommended)
```powershell
# Install Chocolatey (run as Administrator)
Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# Install OpenJDK 11
choco install temurin11
```

#### Option B: Manual Installation
1. Download OpenJDK 11 from [Eclipse Temurin](https://adoptium.net/)
2. Install the downloaded `.msi` file
3. Set JAVA_HOME environment variable:
   - Open System Properties → Advanced → Environment Variables
   - Add new System Variable:
     - Variable name: `JAVA_HOME`
     - Variable value: `C:\Program Files\Eclipse Adoptium\jdk-11.0.xx.xx-hotspot` (adjust version)
   - Edit PATH variable and add: `%JAVA_HOME%\bin`

### Step 2: Verify Java Installation
```cmd
java -version
echo %JAVA_HOME%
```

### Step 3: Install Python and Create Virtual Environment
```cmd
# Download and install Python from python.org if not already installed

# Create virtual environment
python -m venv .venv

# Activate virtual environment
.venv\Scripts\activate
```

### Step 4: Install PySpark
```cmd
# Install PySpark
pip install pyspark==3.5.1

# Install additional dependencies for S3 support
pip install boto3 pandas pyarrow
```

### Step 5: Test Installation
```cmd
python -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')"
```

---

## Troubleshooting

### Common Issues

#### 1. Java Not Found Error
**Error:** `JAVA_HOME is not set`
**Solution:** 
- Verify JAVA_HOME is set correctly
- Restart terminal/command prompt after setting environment variables
- On Windows, ensure you're using the correct path format

#### 2. Python Path Issues
**Error:** `ModuleNotFoundError: No module named 'pyspark'`
**Solution:**
- Ensure virtual environment is activated
- Reinstall PySpark in the correct environment

#### 3. Permission Denied (macOS)
**Error:** Permission issues during installation
**Solution:**
```bash
# Use --user flag for pip install
pip install --user pyspark==3.5.1

# Or fix pip permissions
sudo chown -R $(whoami) $(python -m site --user-site)
```

#### 4. PATH Issues (Windows)
**Error:** `'java' is not recognized as an internal or external command`
**Solution:**
- Add `%JAVA_HOME%\bin` to your PATH environment variable
- Restart Command Prompt/PowerShell

### Verification Script

Create a test file `test_pyspark.py`:

```python
from pyspark.sql import SparkSession
import os

# Create Spark session
spark = SparkSession.builder \
    .appName("TestPySpark") \
    .master("local[*]") \
    .getOrCreate()

# Create sample data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show the data
print("PySpark is working correctly!")
df.show()

# Stop Spark session
spark.stop()
```

Run the test:
```bash
python test_pyspark.py
```

---

## Environment Variables Summary

### macOS/Linux
Add to `~/.zshrc` or `~/.bashrc`:
```bash
export JAVA_HOME="/path/to/your/java"
export PATH="$JAVA_HOME/bin:$PATH"
```

### Windows
System Environment Variables:
- `JAVA_HOME`: `C:\Program Files\Eclipse Adoptium\jdk-11.0.xx.xx-hotspot`
- `PATH`: Add `%JAVA_HOME%\bin`

---

## Next Steps

After successful installation, you can:
1. Run the provided S3 parquet reading scripts
2. Explore Spark DataFrame operations
3. Set up Jupyter Notebook with PySpark
4. Configure AWS credentials for S3 access

For AWS S3 integration, ensure your AWS credentials are configured:
```bash
aws configure
```

---

## Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [AWS S3 with Spark Guide](https://spark.apache.org/docs/latest/cloud-integration.html#amazon-s3)