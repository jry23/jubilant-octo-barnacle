"""
This file contains shared functions and constants for market data preprocessing scripts.
"""
import pandas as pd
import boto3
from pathlib import Path

BUCKET_NAME = "market-data-jyang130"
s3 = boto3.client("s3")

CURRENT_DIR = Path(__file__).resolve().parent
ROOT_DIR = CURRENT_DIR.parent
TICKER_CONFIG_PATH = ROOT_DIR / "config" / "nasdaq.json"

# List all S3 keys for raw equity data for a given ticker
def list_raw_equity_keys(ticker):
    prefix = f"raw/equities/{ticker}/"
    keys = []
    token = None

    while True:
        if token is None:
            response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        else:
            response = s3.list_objects_v2(
                Bucket=BUCKET_NAME, Prefix=prefix, ContinuationToken=token
            )

        keys.extend([obj["Key"] for obj in response.get("Contents", [])])
        token = response.get("NextContinuationToken")

        if not token:
            break

    return keys

# Parse start and end dates from S3 key
def parse_dates_from_key(key):
    parts = key.split("/")
    filename = parts[-1]
    date_part = filename.split("_")[0:2]
    end_date = date_part[1]
    return end_date

# Consolidate multiple CSVs from S3 into a single DataFrame
def consolidate_equity_data(ticker):
    keys = list_raw_equity_keys(ticker)
    
    dated = []
    for key in keys:
        end_date = parse_dates_from_key(key)
        dated.append((end_date, key))
    dated.sort()

    if not dated:
        raise RuntimeError(f"No data found for ticker {ticker} in S3.")
    
    dfs = []
    for end_date, key in dated:
        path = f"s3://{BUCKET_NAME}/{key}"
        df = pd.read_csv(path)
        dfs.append(df)
    
    out = pd.concat(dfs, ignore_index=True)

    out["Date"] = pd.to_datetime(out["Date"])

    out = out.drop_duplicates(subset=["Date"], keep="last")
    out = out.sort_values(by="Date").reset_index(drop=True)

    return out

# Upload consolidated DataFrame back to S3
def upload_consolidated_equity_data(ticker, df):
    key = f"cleaned/equities/{ticker}.csv"
    path = f"s3://{BUCKET_NAME}/{key}"
    df.to_csv(path, index=False)
    print(f"[OK] Uploaded consolidated data for {ticker} to s3://{BUCKET_NAME}/{key}.")