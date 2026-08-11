import io
import pandas as pd
from minio import Minio
import json
# 1. Declare Minio Client
minio_client = Minio(
    "localhost:9000",
    access_key = "minioadmin",
    secret_key = "minioadminpassword",
    secure = False
)

RAW_BUCKET = "crypto-raw-data"
PROCESSED_BUCKET = "crypto-processed-data"

def process_latest_raw_data():
    if not minio_client.bucket_exists(PROCESSED_BUCKET):
        minio_client.make_bucket(PROCESSED_BUCKET)
        print(f"Bucket created{PROCESSED_BUCKET}")
        
    #2. take the list of file in Raw Bucket and choose the latest file 
    objects = list(minio_client.list_objects(RAW_BUCKET, recursive = True ))
# print(f"Amount of files found in bucket{len(objects)}")
# for obj in objects:
#             print(f"file name {obj.object_name}")
#             print(f"last modified {obj.last_modified}")
#             print(f"size {obj.size}")
    if not objects:
        print("can not find any file in raw bucket!")
        return
    

    # arrange the latest taken file followed by fixed time(last_modified)
    latest_object = max(objects, key=lambda x: x.last_modified)
    print(f" reading raw file: {latest_object.object_name}")
    
    #3 . Read bytes from MinIO
    response = minio_client.get_object(RAW_BUCKET, latest_object.object_name)
    json_bytes = response.read()
    response.close()
    response.release_conn()

    #4. Transfer Bytes into string and load it
    json_str = json_bytes.decode('utf-8')
    data_dict = json.loads(json_str)

    #5. Transfer Dict to Dataframe
    df = pd.DataFrame(data_dict)
    
    #choose important columns for this Data Warehouse 
    selected_columns = [
        'id', 'symbol', 'name', 'current_price',
        'market_cap', 'total_volume','high_24h',
        'low_24h' , 'price_change_percentage_24h','last_updated'
    ]    
    df_clean = df[selected_columns].copy()
    
    #transfer the column last_updated into standard Datetime
    df_clean['last_updated'] = pd.to_datetime(df_clean['last_updated'])
    
    #5. transfer DataFrame into parquet in Ram
    parquet_buffer = io.BytesIO()
    df_clean.to_parquet(parquet_buffer, index=False, engine = 'pyarrow')
    parquet_bytes = parquet_buffer.getvalue()
    
    processed_object_name = latest_object.object_name.replace('.json','.parquet')
    
    minio_client.put_object(
        bucket_name = PROCESSED_BUCKET,
        object_name = processed_object_name,
        data = io.BytesIO(parquet_bytes),
        length=len(parquet_bytes),
        content_type = "application/x-parquet"
    )
    
    print(f"Parquet file transfered and saved successfully :{processed_object_name}")
    
if __name__ == "__main__":
    process_latest_raw_data()