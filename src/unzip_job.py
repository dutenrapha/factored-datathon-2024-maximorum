import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = spark._jvm.org.apache.log4j.Logger.getLogger(__name__)
logger.info("Starting the ZIP file processing job")

import boto3
import io
from zipfile import ZipFile

s3 = boto3.client("s3")

bucket = "gdelt-project"
prefix = "bronze/gdelt_data/"
unzip_prefix = "bronze/gdelt_data_unzip/"

logger.info(f"Searching for ZIP files in bucket '{bucket}' with prefix '{prefix}'")

object_keys = []
unzipped_object_keys = []

continuation_token = None
while True:
    if continuation_token:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
    else:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    if 'Contents' in response:
        object_keys.extend([o["Key"] for o in response["Contents"]])
    
    if response.get('IsTruncated'):
        continuation_token = response['NextContinuationToken']
    else:
        break

continuation_token = None
while True:
    if continuation_token:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=unzip_prefix, ContinuationToken=continuation_token)
    else:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=unzip_prefix)
    
    if 'Contents' in response:
        unzipped_object_keys.extend([o["Key"] for o in response["Contents"]])
    
    if response.get('IsTruncated'):
        continuation_token = response['NextContinuationToken']
    else:
        break

if object_keys:
    logger.info(f"Found {len(object_keys)} ZIP files in prefix '{prefix}'   {object_keys[0]}")
else:
    logger.info(f"No ZIP files found in prefix '{prefix}'")

if unzipped_object_keys:
    logger.info(f"Found {len(unzipped_object_keys)} unzipped files in prefix '{unzip_prefix}'   {unzipped_object_keys[0]}")
else:
    logger.info(f"No unzipped files found in prefix '{unzip_prefix}'")

logger.info(f"Objects length {len(object_keys)}")
logger.info(f"Unzipped objects length {len(unzipped_object_keys)}")

for key in object_keys:
    logger.info(f"Starting processing of file {key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    objbuffer = io.BytesIO(obj["Body"].read())
    
    with ZipFile(objbuffer) as zip:
        filenames = zip.namelist()
        for filename in filenames:
            filepath = unzip_prefix + filename
            if filepath not in unzipped_object_keys:
                with zip.open(filename) as file:
                    s3.upload_fileobj(file, bucket, filepath)
                logger.info(f"Processed and uploaded {filename} to {filepath}")

logger.info("Finalizing the ZIP file processing job")
job.commit()
