import io
import json
import pandas as pd
from prefect import task

import config
from utils import get_minio_client, ensure_bucket_exists, get_latest_minio_object, read_object_bytes

SELECTED_COLUMNS = [
    "id", "symbol", "name", "current_price",
    "market_cap", "total_volume", "high_24h",
    "low_24h", "price_change_percentage_24h", "last_updated"
]


@task(name="transform_data", retries=3, retry_delay_seconds=30)
def process_latest_raw_data() -> None:
    """
    Reads the latest raw JSON from MinIO, cleans and selects columns,
    then saves the result as a Parquet file in the processed bucket.
    """
    client = get_minio_client()
    ensure_bucket_exists(client, config.PROCESSED_BUCKET)

    # Get latest raw JSON file
    latest = get_latest_minio_object(client, config.RAW_BUCKET)
    json_bytes = read_object_bytes(client, config.RAW_BUCKET, latest.object_name)

    # Parse JSON → DataFrame
    data = json.loads(json_bytes.decode("utf-8"))
    df = pd.DataFrame(data)

    # Select and clean columns
    df_clean = df[SELECTED_COLUMNS].copy()
    df_clean["last_updated"] = pd.to_datetime(df_clean["last_updated"])

    # Serialize to Parquet in memory
    parquet_buffer = io.BytesIO()
    df_clean.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_bytes = parquet_buffer.getvalue()

    # Upload to processed bucket, keeping same folder path but .parquet extension
    processed_object_name = latest.object_name.replace(".json", ".parquet")
    client.put_object(
        bucket_name=config.PROCESSED_BUCKET,
        object_name=processed_object_name,
        data=io.BytesIO(parquet_bytes),
        length=len(parquet_bytes),
        content_type="application/x-parquet"
    )

    print(f"[Transform] Parquet saved to MinIO: {processed_object_name}")


if __name__ == "__main__":
    process_latest_raw_data()