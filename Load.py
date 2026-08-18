import io
import pandas as pd
from sqlalchemy import text
from prefect import task

import config
from utils import get_minio_client, get_postgres_engine, get_latest_minio_object, read_object_bytes


def create_tables() -> None:
  # Create dim_coins and fact_market_prices table
    engine = get_postgres_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_coins (
                coin_id   VARCHAR PRIMARY KEY,
                name      VARCHAR,
                symbol    VARCHAR
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_market_prices (
                id                      SERIAL PRIMARY KEY,
                coin_id                 VARCHAR REFERENCES dim_coins(coin_id),
                current_price           FLOAT,
                market_cap              BIGINT,
                total_volume            BIGINT,
                high_24h                FLOAT,
                low_24h                 FLOAT,
                price_change_pct_24h    FLOAT,
                timestamp               TIMESTAMP
            )
        """))
        conn.commit()
    print("[Load] Tables created (or already exist)")


@task(name="load_data", retries=3, retry_delay_seconds=30)
def load_data() -> None:
    """
    Reads the latest Parquet from MinIO, splits it into
    dim_coins and fact_market_prices, then loads into PostgreSQL.
    """
    create_tables()

    client  = get_minio_client()
    engine  = get_postgres_engine()

    # Get latest processed Parquet file
    latest       = get_latest_minio_object(client, config.PROCESSED_BUCKET)
    parquet_bytes = read_object_bytes(client, config.PROCESSED_BUCKET, latest.object_name)

    # Parse Parquet → DataFrame
    df = pd.read_parquet(io.BytesIO(parquet_bytes))

    # ── dim_coins: upsert (only insert new coins) ────────────────────────────
    df_dim = df[["id", "name", "symbol"]].drop_duplicates()
    df_dim = df_dim.rename(columns={"id": "coin_id"})

    with engine.connect() as conn:
        existing = pd.read_sql("SELECT coin_id FROM dim_coins", conn)

    df_dim_new = df_dim[~df_dim["coin_id"].isin(existing["coin_id"])]
    if not df_dim_new.empty:
        df_dim_new.to_sql("dim_coins", engine, if_exists="append", index=False)
        print(f"[Load] Inserted {len(df_dim_new)} new coins into dim_coins")
    else:
        print("[Load] No new coins to insert into dim_coins")

    # fact_market_prices
    df_fact = df[[
        "id", "current_price", "market_cap", "total_volume",
        "high_24h", "low_24h", "price_change_percentage_24h", "last_updated"
    ]].copy()

    df_fact = df_fact.rename(columns={
        "id": "coin_id",
        "price_change_percentage_24h": "price_change_pct_24h",
        "last_updated": "timestamp"
    })

    df_fact.to_sql("fact_market_prices", engine, if_exists="append", index=False)
    print(f"[Load] Inserted {len(df_fact)} rows into fact_market_prices")


if __name__ == "__main__":
    load_data()