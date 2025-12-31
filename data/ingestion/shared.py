"""
This file contains shared functions and constants for market data ingestion scripts.
"""
import json
from pathlib import Path

import pandas as pd
import yfinance as yf

BUCKET_NAME = "market-data-jyang130"

# Resolve config paths
CURRENT_DIR = Path(__file__).resolve().parent
ROOT_DIR = CURRENT_DIR.parent
TICKER_CONFIG_PATH = ROOT_DIR / "config" / "nasdaq.json"

# URL for factor data
FACTOR_URL = (
    "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"
    "F-F_Research_Data_5_Factors_2x3_daily_CSV.zip"
)

# Load tickers from config, makes it easier to add/remove in the future
def load_tickers(config_path=TICKER_CONFIG_PATH):
    with open(config_path, "r") as f:
        config = json.load(f)
    return config.get("NASDAQ", [])

# Fetch historical price data using yfinance
def fetch_OHCLV_daily(ticker, start, end):
    print(f"[INFO] Fetching {ticker} data from {start} to {end}...")
    df = yf.download(ticker, start=start, end=end, progress=False)

    if df.empty:
        print(f"[WARN] No data found for {ticker}, skipping.")
        return df
    
    df.columns = df.columns.get_level_values(0)
    df = df.reset_index()
    df.columns.name = None

    return df

# Download FF5 daily factor CSV text
def download_ff5_daily(url=FACTOR_URL):
    print(f"[INFO] Downloading FF5 daily factors from {url} ...")
    print(url)
    df = pd.read_csv(url, compression='zip', skiprows=3)
    df = df.rename(columns={"Unnamed: 0": "Date"})
    df = df[pd.to_numeric(df["Date"], errors="coerce").notna()]
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')

    return df

# Upload data to S3 bucket
def upload_to_s3(datatype, df, ticker, start, end):
    if datatype == "equities":
        if not (ticker and start and end):
            raise ValueError("For EQUITIES, ticker, start, and end are required.")
        key = f"raw/equities/{ticker}/{ticker}_{start}_{end}.csv"
    elif datatype == "factors":
        key = "raw/factors/FF5_daily.csv"
    else:
        raise ValueError(f"Unknown datatype: {datatype}")

    print(f"[INFO] Uploading ({datatype}) to s3://{BUCKET_NAME}/{key}...")

    df.to_csv(f"s3://{BUCKET_NAME}/{key}", index=False)

    print(f"[OK] Uploaded successfully.")

# Determine the last date for which data is available for a given equity ticker in S3
def get_last_covered_date(ticker):
    path = f"s3://{BUCKET_NAME}/clean/equities/{ticker}.csv"
    try:
        df = pd.read_csv(path, usecols=["Date"])
    except FileNotFoundError:
        return None
    df["Date"] = pd.to_datetime(df["Date"])
    return df["Date"].max().date()