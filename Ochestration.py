import os
from datetime import timedelta
from prefect import flow, get_run_logger

os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"

from Fetch import fetch_crypto_data
from transform import process_latest_raw_data
from Load import load_data


@flow(name="Crypto ETL Pipeline")
def crypto_etl_flow() -> None:
    logger = get_run_logger()

    logger.info("Pipeline started")
    fetch_crypto_data()

    logger.info("Fetch complete. Starting transform...")
    process_latest_raw_data()

    logger.info("Transform complete. Starting load...")
    load_data()

    logger.info("Pipeline completed successfully!")


if __name__ == "__main__":
    crypto_etl_flow.serve(
        name="crypto-etl-hourly",
        interval=timedelta(hours=1),
        tags=["crypto", "etl"]
    )