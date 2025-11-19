from datetime import date, timedelta

from .shared import (
    load_tickers,
    fetch_historical_data,
    upload_to_s3,
    get_last_covered_date,
)

"""
This script updates the market data for a list of tickers by fetching new data
since the last available date in the S3 bucket and uploading it.
"""

def main():
    today = date.today()
    tickers = load_tickers()

    print(f"[INFO] Running local incremental update for {len(tickers)} tickers on {today}.")

    updated = []

    for ticker in tickers:
        print(f"\n[INFO] Processing {ticker}...")

        last_date = get_last_covered_date(ticker)

        if last_date is None:
            print(f"[WARN] No existing data for {ticker}. "
                  f"Run upload_initial.py first to fetch history.")
            continue

        start_date = last_date + timedelta(days=1)
        if start_date > today:
            print(f"[INFO] {ticker} already up to date through {last_date}.")
            continue

        start_str = start_date.isoformat()
        end_str = today.isoformat()
        print(f"[INFO] Updating {ticker} from {start_str} to {end_str}...")

        df = fetch_historical_data(ticker, start_str, end_str)
        if df.empty:
            print(f"[WARN] No new rows for {ticker} in this range.")
            continue

        upload_to_s3(df, ticker, start_str, end_str)
        updated.append(ticker)

    print(f"\n[OK] Incremental update complete. Updated tickers: {updated}")


if __name__ == "__main__":
    main()