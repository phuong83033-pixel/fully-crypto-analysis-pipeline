import json
import requests
from datetime import datetime, timezone
from io import BytesIO
from prefect import task

import config
from utils import get_minio_client, ensure_bucket_exists


@task(name="fetch_raw_data", retries=3, retry_delay_seconds=30)
def fetch_crypto_data() -> None:
    client = get_minio_client()
    ensure_bucket_exists(client, config.RAW_BUCKET)

    # Call API
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": config.TOP_N_COINS,
        "page": 1,
        "sparkline": "false"
    }
    response = requests.get(config.COINGECKO_URL, params=params)
    response.raise_for_status()  # raises exception if status != 200

    data = response.json()

    # Build object name: coingecko/YYYY/MM/DD/crypto_market_HHMMSS.json
    now = datetime.now(timezone.utc)
    object_name = f"coingecko/{now.strftime('%Y/%m/%d')}/crypto_market_{now.strftime('%H%M%S')}.json"

    # Serialize and upload
    json_bytes = json.dumps(data, indent=2).encode("utf-8")
    client.put_object(
        bucket_name=config.RAW_BUCKET,
        object_name=object_name,
        data=BytesIO(json_bytes),
        length=len(json_bytes),
        content_type="application/json"
    )

    print(f"[Fetch] Raw JSON saved to MinIO: {object_name}")


if __name__ == "__main__":
    fetch_crypto_data()