import boto3
import json
import time
import requests
from dotenv import load_dotenv
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Load environment variables from .env file to keep sensitive information secure
load_dotenv()

# AWS configurations (loaded from .env)
region = os.getenv("AWS_REGION")  # Default to "us-east-1" if not provided
bucket_name = os.getenv("S3_BUCKET_NAME")  # Default bucket name
glue_database_name = os.getenv("GLUE_DATABASE_NAME")  # Default Glue database name
athena_output_location = f"s3://{bucket_name}/athena-results/"  # Construct Athena output location based on bucket

# Sportsdata.io configurations (loaded from .env)
api_key = os.getenv("SPORTS_DATA_API_KEY")  # Get API key from .env
nba_endpoint = os.getenv("NBA_ENDPOINT")  # Get NBA endpoint from .env

# Create AWS service clients for S3, Glue, and Athena
s3_client = boto3.client("s3", region_name=region)
glue_client = boto3.client("glue", region_name=region)
athena_client = boto3.client("athena", region_name=region)


def create_s3_bucket():
    """Create an S3 bucket for storing sports data."""
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"S3 bucket '{bucket_name}' created successfully.")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print(f"S3 bucket '{bucket_name}' already exists and is owned by you.")
    except NoCredentialsError:
        print("AWS credentials not found. Please configure your credentials.")
    except Exception as e:
        print(f"Error creating S3 bucket: {e}")


def create_glue_database():
    """Create a Glue database for storing data lake tables."""
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": glue_database_name,
                "Description": "Glue database for NBA sports analytics.",
            }
        )
        print(f"Glue database '{glue_database_name}' created successfully.")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Glue database '{glue_database_name}' already exists.")
    except Exception as e:
        print(f"Error creating Glue database: {e}")


def fetch_nba_data():
    """Fetch NBA player data from sportsdata.io."""
    try:
        headers = {"Ocp-Apim-Subscription-Key": api_key}
        response = requests.get(nba_endpoint, headers=headers)
        response.raise_for_status()  # Check for HTTP request errors
        print("Fetched NBA data successfully.")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.ConnectionError:
        print("Network connection error. Please check your internet connection.")
    except requests.exceptions.Timeout:
        print("Request timed out. Please try again.")
    except requests.exceptions.RequestException as err:
        print(f"An error occurred: {err}")
    except Exception as e:
        print(f"Error fetching NBA data: {e}")
    return []


def convert_to_line_delimited_json(data):
    """Convert a list of dictionaries to line-delimited JSON format."""
    print("Converting data to line-delimited JSON format...")
    try:
        return "\n".join([json.dumps(record) for record in data])
    except TypeError as e:
        print(f"Error converting data to JSON: {e}")
        return ""


def upload_data_to_s3(data):
    """Upload NBA data to the specified S3 bucket."""
    try:
        # Convert data to line-delimited JSON format
        line_delimited_data = convert_to_line_delimited_json(data)
        file_key = "raw-data/nba_player_data.jsonl"  # S3 key for the uploaded file

        # Upload the file to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=line_delimited_data,
        )
        print(f"Uploaded data to S3: {file_key}")
    except NoCredentialsError:
        print("AWS credentials not found. Please configure your credentials.")
    except Exception as e:
        print(f"Error uploading data to S3: {e}")


def create_glue_table():
    """Create a Glue table for the NBA data."""
    try:
        glue_client.create_table(
            DatabaseName=glue_database_name,
            TableInput={
                "Name": "nba_players",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "PlayerID", "Type": "int"},
                        {"Name": "FirstName", "Type": "string"},
                        {"Name": "LastName", "Type": "string"},
                        {"Name": "Team", "Type": "string"},
                        {"Name": "Position", "Type": "string"},
                        {"Name": "Points", "Type": "int"},
                    ],
                    "Location": f"s3://{bucket_name}/raw-data/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                    },
                },
                "TableType": "EXTERNAL_TABLE",
            },
        )
        print("Glue table 'nba_players' created successfully.")
    except glue_client.exceptions.AlreadyExistsException:
        print("Glue table 'nba_players' already exists.")
    except Exception as e:
        print(f"Error creating Glue table: {e}")


def configure_athena():
    """Configure Athena with the output location for query results."""
    try:
        athena_client.start_query_execution(
            QueryString="CREATE DATABASE IF NOT EXISTS nba_analytics",
            QueryExecutionContext={"Database": glue_database_name},
            ResultConfiguration={"OutputLocation": athena_output_location},
        )
        print("Athena output location configured successfully.")
    except Exception as e:
        print(f"Error configuring Athena: {e}")


def main():
    """Main workflow to set up the NBA sports analytics data lake."""
    print("Starting the data lake setup process...")
    create_s3_bucket()
    time.sleep(5)  # Ensure S3 bucket creation has propagated
    create_glue_database()
    nba_data = fetch_nba_data()
    if nba_data:
        upload_data_to_s3(nba_data)
        create_glue_table()
        configure_athena()
    print("Data lake setup complete.")


if __name__ == "__main__":
    main()
