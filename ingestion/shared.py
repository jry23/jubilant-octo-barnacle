import io
import json
from datetime import date
from pathlib import Path
from typing import List, Optional

import boto3
import pandas as pd
import yfinance as yf

"""
This file contains shared functions and constants for market data ingestion scripts.
"""

BUCKET_NAME = "market-data-jyang130"

# Resolve config paths
CURRENT_DIR = Path(__file__).resolve().parent
ROOT_DIR = CURRENT_DIR.parent
TICKER_CONFIG_PATH = ROOT_DIR / "config" / "tickers.json"

# Shared S3 client
s3 = boto3.client("s3")

# Load tickers from config, makes it easier to add/remove in the future
def load_tickers(config_path: Path = TICKER_CONFIG_PATH) -> List[str]:
    with open(config_path, "r") as f:
        config = json.load(f)
    return config.get("equities", [])

# Fetch historical price data using yfinance
def fetch_historical_data(ticker: str, start: str, end: str) -> pd.DataFrame:
    print(f"[INFO] Fetching {ticker} data from {start} to {end}...")
    df = yf.download(ticker, start=start, end=end, progress=False)

    if df.empty:
        print(f"[WARN] No data found for {ticker}, skipping.")
        return df

    df.index.name = "Date"
    df.reset_index(inplace=True)
    return df

# Upload data to S3 bucket
def upload_to_s3(df: pd.DataFrame, ticker: str, start: str, end: str):
    key = f"raw/equities/{ticker}/{start}_{end}_{ticker}_raw.csv"
    print(f"[INFO] Uploading {ticker} data to s3://{BUCKET_NAME}/{key}...")

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=csv_buffer.getvalue()
    )

    print(f"[OK] Uploaded {ticker} data successfully.")

# Determine the last date for which data is available for a given ticker in S3
def get_last_covered_date(ticker: str) -> Optional[date]:
    prefix = f"raw/equities/{ticker}/"
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

    last_dates = []
    for obj in response["Contents"]:
        filename = obj["Key"].split("/")[-1]
        parts = filename.split("_")
        if len(parts) < 3:
            continue
        end_date_str = parts[1]
        try:
            end_date = date.fromisoformat(end_date_str)
            last_dates.append(end_date)
        except ValueError:
            continue

    return max(last_dates) if last_dates else None