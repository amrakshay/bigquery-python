# BigQuery Python Example

This repository demonstrates how to connect to BigQuery from Python. The example requires Python 3.10.

## Prerequisites

### 1. Granting Access to User

1. Log in to the GCP console with an admin user
2. Navigate to IAM & Admin => IAM
3. Click on "Grant access" and grant the following roles to your user:
   - BigQuery Read Session User
   - Service Usage Consumer
   - BigQuery Admin (for simplicity of this example)

> Note: The above roles are minimal requirements to run queries on BigQuery. You can grant additional permissions based on your specific needs.

### 2. Creating Credentials

1. Log in to the GCP console with the user you want to use for the Python example
2. Open Cloud Shell and run:
   ```bash
   gcloud auth application-default login
   ```
3. Follow the on-screen instructions
4. After completion, you'll see output similar to:
   ```
   Credentials saved to file: [/tmp/tmp.VHezsFMnx6/application_default_credentials.json]
   ```
5. Copy the contents of the credentials file to your local project's `authorized_user.json` file

## Running the Example

1. Open `main.py` and update the following:
   - Your user email
   - Project ID
   - SQL query according to your data
2. Run the example:
   ```bash
   python3 main.py
   ```