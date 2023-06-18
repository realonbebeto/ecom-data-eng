import boto3
from botocore import UNSIGNED
from botocore.client import Config
from typing import List



def download_files(bucket_name:str, files: List[str]):
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    bucket_name = "d2b-internal-assessment-bucket"
    for file in files:
        s3.download_file(bucket_name, f"orders_data/{file}", "../data/{file}")

# response = s3.list_objects(Bucket=bucket_name, Prefix="orders_data")