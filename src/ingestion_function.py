import boto3
import requests
from bs4 import BeautifulSoup
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

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

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_file, zip_file) for zip_file in zip_links]
        results = [future.result() for future in futures]

    new_files = sum(results)
    logger.info(f"Successfully uploaded {new_files} new files to S3")
    return {
        'statusCode': 200,
        'body': f'Successfully uploaded {new_files} new files to S3'
    }

def process_file(zip_file):
    if not file_exists_in_s3(zip_file):
        zip_url = f'http://data.gdeltproject.org/events/{zip_file}'
        return download_and_upload_to_s3(zip_url, zip_file)
    else:
        logger.info(f"File {zip_file} already exists in S3, skipping download.")
        return 0

def file_exists_in_s3(file_name):
    """Check if the file already exists in the S3 bucket"""
    try:
        s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=f'bronze/gdelt_data/{file_name}')
        return True
    except:
        return False

def download_and_upload_to_s3(zip_url, zip_file):
    """Download the ZIP file and upload it to S3 using streaming"""
    try:
        with requests.Session() as session:
            retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[502, 503, 504])
            session.mount('http://', HTTPAdapter(max_retries=retries))
            response = session.get(zip_url, stream=True)
            if response.status_code == 200:
                s3_key = f'bronze/gdelt_data/{zip_file}'
                s3_client.upload_fileobj(response.raw, S3_BUCKET_NAME, s3_key)
                logger.info(f"Successfully uploaded {zip_file} to S3")
                return 1
            else:
                logger.error(f"Failed to download {zip_file} from {zip_url}: {response.status_code}")
                return 0
    except Exception as e:
        logger.error(f"Error downloading or uploading file {zip_file}: {e}")
        return 0
