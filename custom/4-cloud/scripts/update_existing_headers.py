#!/usr/bin/env python3
"""
Update Content-Type headers for all existing .pmtiles files in R2 bucket.
This script copies each object to itself with updated metadata.
"""
import boto3
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file in repository root
repo_root = Path(__file__).resolve().parent.parent.parent
env_file = repo_root / '.env'

print(f"Loading .env from: {env_file}")
if not env_file.exists():
    raise FileNotFoundError(f".env file not found at {env_file}")

load_dotenv(env_file)

# Get R2 credentials from environment
R2_ACCESS_KEY_ID = os.getenv('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.getenv('R2_SECRET_ACCESS_KEY')
R2_ENDPOINT = os.getenv('R2_ENDPOINT')
R2_BUCKET_NAME = os.getenv('R2_BUCKET_NAME', 'grid3-tiles')

# Debug output
print(f"R2_ACCESS_KEY_ID: {'✓' if R2_ACCESS_KEY_ID else '✗'}")
print(f"R2_SECRET_ACCESS_KEY: {'✓' if R2_SECRET_ACCESS_KEY else '✗'}")
print(f"R2_ENDPOINT: {R2_ENDPOINT if R2_ENDPOINT else '✗'}")
print(f"R2_BUCKET_NAME: {R2_BUCKET_NAME}")
print()

# Validate credentials
if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT]):
    raise ValueError("Missing R2 credentials. Please check your .env file.")

# Create S3 client
s3 = boto3.client('s3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name='auto'
)

def update_object_metadata(bucket, key, content_type='application/vnd.pmtiles'):
    """
    Update an object's metadata by copying it to itself with new headers.
    """
    try:
        # Get current object metadata
        head = s3.head_object(Bucket=bucket, Key=key)
        current_content_type = head.get('ContentType', 'unknown')
        
        print(f"Updating: {key}")
        print(f"  Current Content-Type: {current_content_type}")
        
        # Copy object to itself with new metadata
        copy_source = {'Bucket': bucket, 'Key': key}
        s3.copy_object(
            Bucket=bucket,
            CopySource=copy_source,
            Key=key,
            MetadataDirective='REPLACE',
            ContentType=content_type,
            # Preserve the storage class
            StorageClass=head.get('StorageClass', 'STANDARD')
        )
        
        print(f"  New Content-Type: {content_type} ✓")
        return True
        
    except Exception as e:
        print(f"  Error: {e} ✗")
        return False

# List all objects in the bucket
print(f"Listing objects in bucket: {R2_BUCKET_NAME}")
print("-" * 60)

try:
    response = s3.list_objects_v2(Bucket=R2_BUCKET_NAME)
    
    if 'Contents' not in response:
        print("No objects found in bucket.")
    else:
        objects = response['Contents']
        pmtiles_objects = [obj for obj in objects if obj['Key'].endswith('.pmtiles')]
        
        print(f"Found {len(pmtiles_objects)} .pmtiles file(s)")
        print()
        
        success_count = 0
        for obj in pmtiles_objects:
            if update_object_metadata(R2_BUCKET_NAME, obj['Key']):
                success_count += 1
            print()
        
        print("-" * 60)
        print(f"Updated {success_count}/{len(pmtiles_objects)} objects successfully")
        
except Exception as e:
    print(f"Error listing objects: {e}")
