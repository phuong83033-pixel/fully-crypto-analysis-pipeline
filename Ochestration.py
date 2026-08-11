import os
os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
from prefect import flow , task
from datetime import timedelta
from FetchRawData import fetch_crypto_data
from transform_data import process_latest_raw_data
from Loading import load_data


@flow(name = "Crypto ETL Pipeline")
def crypto_etl_flow():
    try :
        print("Pipeline start .....")
        # Step 1: Fetch raw data from API → save JSON to MinIO
        fetch_crypto_data()
        # Step 2: Read JSON from MinIO → transform → save Parquet to MinIO
        process_latest_raw_data()
        # Step 3: Read Parquet from MinIO → load into PostgreSQL
        load_data()
        print("Pipeline completed successfully!")
    
    except Exception as e:
        print(f"Pipeline failure at task : {str(e)}")
        raise e 
    
if __name__ == "__main__":
    crypto_etl_flow.serve(
        name = "crypto-etl-hourly-development",
        interval=timedelta(hours=1),
        tags=["crypto","production"]
    )