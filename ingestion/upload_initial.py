from datetime import date

from .shared import (
    load_tickers,
    fetch_historical_data,
    upload_to_s3,
)

"""
This script uploads initial historical market data for a list of tickers to
the S3 bucket.
"""
# Around 10 years of data to start with
START_DATE = "2015-01-01"
END_DATE = date.today().isoformat()

def main():
    tickers = load_tickers()
    print(f"[INFO] Loaded {len(tickers)} tickers: {tickers}")

    for ticker in tickers:
        df = fetch_historical_data(ticker, START_DATE, END_DATE)
        if df.empty:
            print(f"[WARN] Skipping {ticker} (no data).")
            continue
        
        upload_to_s3(df, ticker, START_DATE, END_DATE)
    
    print("\n[OK] Initial upload complete.")

if __name__ == "__main__":
    main()