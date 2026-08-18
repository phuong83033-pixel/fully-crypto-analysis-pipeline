# utilities.py 

import io
from minio import Minio
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from minio.api import Object

import config

#connect minio_client
def get_minio_client() -> Minio:
    return Minio(
        config.MINIO_ENDPOINT,
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=config.MINIO_SECURE
    )

#connect postgres_engine
def get_postgres_engine() -> Engine:
    return create_engine(config.POSTGRES_CONN_STR)

#check if there is bucket yet 
def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket created: {bucket_name}")

#get the latest file from minio
def get_latest_minio_object(client: Minio, bucket_name: str) -> Object:
   
    objects = list(client.list_objects(bucket_name, recursive=True))
    if not objects:
        raise ValueError(f"No files found in bucket: {bucket_name}")

    latest = max(objects, key=lambda obj: obj.last_modified)
    print(f"Latest file found: {latest.object_name}")
    return latest

#Return into bytes from the old object 
def read_object_bytes(client: Minio, bucket_name: str, object_name: str) -> bytes:
    response = client.get_object(bucket_name, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    return data