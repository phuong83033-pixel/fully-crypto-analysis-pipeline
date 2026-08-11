import io 
import pandas as pd
from sqlalchemy import create_engine, text
from minio import Minio

# 1. Declare MinIO Client
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadminpassword",
    secure=False
)

BUCKET_NAME = "crypto-processed-data"

engine = create_engine("postgresql+psycopg2://de_user:de_password@localhost:5432/crypto_dw")

# 2. Create tables if not exists
def create_tables():
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
        print("Tables created successfully (or already exist)")

# 3. Read latest Parquet from MinIO, split and load into PostgreSQL
def load_data():
    create_tables()
    
    # 3a. List all files and find the latest one (same as transform_data.py)
    objects = list(minio_client.list_objects(BUCKET_NAME, recursive=True))
    if not objects:
        print("No parquet files found in processed bucket!")
        return
    
    latest_object = max(objects, key=lambda x: x.last_modified)
    print(f"Reading processed file: {latest_object.object_name}")
    
    # 3b. Read bytes from MinIO
    response = minio_client.get_object(BUCKET_NAME, latest_object.object_name)
    parquet_bytes = response.read()
    response.close()
    response.release_conn()
    
    # 3c. Parse Parquet into DataFrame
    df = pd.read_parquet(io.BytesIO(parquet_bytes))
    
    # 4. Split into dim_coins
    df_dim = df[['id', 'name', 'symbol']].drop_duplicates()
    df_dim.rename(columns={'id': 'coin_id'}, inplace=True)
    
    # Check which coins already exist in DB to avoid duplicates
    with engine.connect() as conn:
        existing = pd.read_sql("SELECT coin_id FROM dim_coins", conn)
    
    df_dim_new = df_dim[~df_dim['coin_id'].isin(existing['coin_id'])]
    
    if not df_dim_new.empty:
        df_dim_new.to_sql('dim_coins', engine, if_exists='append', index=False)
        print(f"Inserted {len(df_dim_new)} new coins into dim_coins")
    else:
        print("No new coins to insert into dim_coins")
    
    # 5. Split into fact_market_prices
    df_fact = df[['id', 'current_price', 'market_cap', 'total_volume',
                  'high_24h', 'low_24h', 'price_change_percentage_24h',
                  'last_updated']].copy()
    
    df_fact.rename(columns={
        'id': 'coin_id',
        'price_change_percentage_24h': 'price_change_pct_24h',
        'last_updated': 'timestamp'
    }, inplace=True)
    
    df_fact.to_sql('fact_market_prices', engine, if_exists='append', index=False)
    print(f"Inserted {len(df_fact)} rows into fact_market_prices")

if __name__ == "__main__":
    load_data()