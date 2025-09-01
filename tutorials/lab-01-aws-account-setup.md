# AWS Account Setup and CLI Configuration Tutorial (Classroom Edition)

## 1. Creating an AWS Account

### Step 1: Go to AWS

* Open [aws.amazon.com](https://aws.amazon.com)
* Click **Create an AWS Account**

### Step 2: Enter Details

* **Email & Password** → for the root account
* **AWS Account Name** → your name or company name

### Step 3: Contact & Payment Info

* Provide your address and phone number
* Add a valid **credit/debit card** (required, even for free tier)

  > AWS may place a small temporary charge for verification

### Step 4: Verify Identity

* Confirm via **SMS** or **voice call**

### Step 5: Support Plan

* Select **Basic Support – Free**

### Step 6: Sign In

* Log in with your email and password at [AWS Console](https://aws.amazon.com/console)

---

## 2. Credentials for CLI

For classroom speed, we’ll create **access keys** under the root account.

⚠️ **Note**: In real projects, you should create an **IAM user** instead. Using the root user for CLI is not recommended outside of training.

### Steps

1. Log in to AWS Console
2. Click your account name (top right) → **Security Credentials**
3. Scroll to **Access keys** → **Create access key**
4. Select **Command Line Interface (CLI)** as the use case
5. Confirm, then click **Create access key**
6. Copy both:

   * **Access Key ID**
   * **Secret Access Key** (only shown once)
7. Download the `.csv` file and keep it safe

---

## 3. Install AWS CLI

### Windows

* Download: [AWS CLI for Windows](https://awscli.amazonaws.com/AWSCLIV2.msi)
* Run the installer → **Install**

### macOS

* Download: [AWS CLI for macOS](https://awscli.amazonaws.com/AWSCLIV2.pkg)
* Open the file → **Install**

### Verify

```bash
aws --version
```

You should see something like:

```
aws-cli/2.x.x
```

---

## 4. Configure AWS CLI

Run:

```bash
aws configure
```

Enter:

* **AWS Access Key ID**
* **AWS Secret Access Key**
* **Default region** → `us-east-1`
* **Default output format** → `json` (or `table`)

---

## 5. Test Your Setup

Check your account:

```bash
aws sts get-caller-identity
```

List your buckets (empty is normal for new accounts):

```bash
aws s3 ls
```

List regions:

```bash
aws ec2 describe-regions --output table
```

---

## 6. Quick S3 Hands-On

```bash
# Create a bucket (name must be unique and lowercase)
aws s3 mb s3://my-class-bucket-2025

# Upload a file
aws s3 cp notes.txt s3://my-class-bucket-2025/

# Download the file back
aws s3 cp s3://my-class-bucket-2025/notes.txt ./
```

---

## 7. Security Reminder

* Root keys are used here **only for class practice**
* In real projects:

  * Create an **IAM user** for CLI use
  * Enable **MFA** for root login
  * Never commit keys to GitHub

---

## 8. Next Steps

1. Practice S3 uploads/downloads
2. Explore the AWS Console visually
3. Try CLI with EC2, Lambda, IAM, etc.
4. Learn both Console (UI) and CLI (automation)