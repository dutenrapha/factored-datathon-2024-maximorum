import boto3
import requests
from bs4 import BeautifulSoup
import logging
import os

s3_client = boto3.client('s3')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']  

GDELT_URL = 'http://data.gdeltproject.org/events/index.html'

def lambda_handler(event, context):
    response = requests.get(GDELT_URL)
    if response.status_code != 200:
        logger.error(f"Failed to retrieve GDELT file list: {response.status_code}")
        return {'statusCode': response.status_code, 'body': 'Failed to retrieve file list'}

    soup = BeautifulSoup(response.text, 'html.parser')
    zip_links = [link['href'] for link in soup.find_all('a', href=True) if link['href'].endswith('.zip')]
    logger.info(f"Found {len(zip_links)} files to check.")

    new_files = 0
    for zip_file in zip_links:
        if not file_exists_in_s3(zip_file):
            zip_url = f'http://data.gdeltproject.org/events/{zip_file}'
            download_and_upload_to_s3(zip_url, zip_file)
            new_files += 1
        else:
            logger.info(f"File {zip_file} already exists in S3, skipping download.")

    logger.info(f"Successfully uploaded {new_files} new files to S3")
    return {
        'statusCode': 200,
        'body': f'Successfully uploaded {new_files} new files to S3'
    }

def file_exists_in_s3(file_name):
    """Check if the file already exists in the S3 bucket"""
    try:
        s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=f'bronze/gdelt_data/{file_name}')
        return True
    except:
        return False

def download_and_upload_to_s3(zip_url, zip_file):
    """Download the ZIP file and upload it to S3"""
    try:
        zip_response = requests.get(zip_url)
        if zip_response.status_code == 200:
            s3_key = f'bronze/gdelt_data/{zip_file}'
            s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=zip_response.content)
            logger.info(f"Successfully uploaded {zip_file} to S3")
        else:
            logger.error(f"Failed to download {zip_file} from {zip_url}: {zip_response.status_code}")
    except Exception as e:
        logger.error(f"Error downloading or uploading file {zip_file}: {e}")
        raise

