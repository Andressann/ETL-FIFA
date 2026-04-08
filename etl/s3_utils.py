import os
import boto3
from pathlib import Path

def get_s3_client():
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )

def download_csv_from_s3(local_path: str) -> str:
    bucket = os.getenv("S3_BUCKET")
    key = os.getenv("S3_SOURCE_KEY", "raw/FullData.csv")
    if not bucket:
        raise ValueError("S3_BUCKET env var is required")
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    get_s3_client().download_file(bucket, key, local_path)
    print(f"Downloaded s3://{bucket}/{key} -> {local_path}")
    return local_path

def upload_processed_to_s3(local_path: str) -> None:
    bucket = os.getenv("S3_BUCKET")
    key = os.getenv("S3_OUTPUT_KEY", "processed/players_output.csv")
    if not bucket:
        return
    get_s3_client().upload_file(local_path, bucket, key)
    print(f"Uploaded {local_path} -> s3://{bucket}/{key}")
