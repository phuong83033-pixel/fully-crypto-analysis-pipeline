import json
import requests
from datetime import datetime,timezone
from io import BytesIO
from minio import Minio

# 1.connect to MinIO Client
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadminpassword",
    secure=False
)

BUCKET_NAME = "crypto-raw-data"

def fetch_crypto_data():
    # Auto-create bucket if not exists
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)
        print(f"Bucket created: {BUCKET_NAME}")
    
    # 2. call API take top 20 Coin
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 20,
        "page": 1,
        "sparkline": "false"
    }
    
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Lỗi gọi API: {response.status_code}")
        return
    
    data = response.json()
    
    # 3. Create file in base of YYYY/MM/DD/HH.json
    now = datetime.now(timezone.utc)
    object_name = f"coingecko/{now.strftime('%Y/%m/%d')}/crypto_market_{now.strftime('%H%M%S')}.json"
    
    # transfer data into bytes
    json_bytes = json.dumps(data, indent=2).encode('utf-8')
    data_stream = BytesIO(json_bytes)
    
    # 4. Push directly file Raw JSON lên MinIO
    minio_client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        data=data_stream,
        length=len(json_bytes),
        content_type="application/json"
    )
    
    print(f" Đã lưu file thô thành công vào MinIO: {object_name}")

if __name__ == "__main__":
    fetch_crypto_data()