import json

from shared import (
    TICKER_CONFIG_PATH,
    consolidate_equity_data,
    upload_consolidated_equity_data,
)

def load_tickers():
    with open(TICKER_CONFIG_PATH, "r") as f:
        config = json.load(f)
    return config.get("NASDAQ", [])

def main():
    tickers = load_tickers()
    print(f"[INFO] Consolidating {len(tickers)} tickers from {TICKER_CONFIG_PATH}")

    ok, fail = [], []

    for ticker in tickers:
        try:
            print(f"\n[INFO] {ticker}: consolidating raw chunks...")
            df = consolidate_equity_data(ticker)

            print(f"[INFO] {ticker}: uploading cleaned file...")
            upload_consolidated_equity_data(ticker, df)

            ok.append(ticker)
        except Exception as e:
            print(f"[FAIL] {ticker}: {e}")
            fail.append(ticker)

    print(f"\n[SUMMARY] OK={len(ok)} FAIL={len(fail)}")
    if fail:
        print("[SUMMARY] Failed tickers:", fail)


if __name__ == "__main__":
    main()