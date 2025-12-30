from datetime import date

from shared import (
    load_tickers,
    fetch_OHCLV_daily,
    upload_to_s3,
    get_last_covered_date
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
        last_date = get_last_covered_date(ticker)
        if last_date is not None:
            print(f"[INFO] Skipping {ticker} — already has initial data in S3 (latest date: {last_date})")
            continue

        df = fetch_OHCLV_daily(ticker, START_DATE, END_DATE)
        if df.empty:
            print(f"[WARN] Skipping {ticker} (no data).")
            continue

        upload_to_s3("equities", df, ticker, START_DATE, END_DATE)

    print("\n[OK] Initial upload complete.")

if __name__ == "__main__":
    main()