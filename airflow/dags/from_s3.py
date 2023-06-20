import boto3
from botocore import UNSIGNED
from botocore.client import Config
from typing import List
import os

DIR = os.getcwd()

def download_files(bucket_name:str, files: List[str]):
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    for file in files:
        TO_FILE = os.path.join(DIR, f"tmp_data/{file}")
        s3.download_file(bucket_name, f"orders_data/{file}", TO_FILE)
        print(f"{file} downloaded as {TO_FILE}")

# response = s3.list_objects(Bucket=bucket_name, Prefix="orders_data")