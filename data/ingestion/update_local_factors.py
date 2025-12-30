from datetime import date

from shared import (
    download_ff5_daily,
    upload_to_s3,
)

"""
This script updates (overwrites) the Fama-French FF5 daily factor file in S3.
We overwrite the same key every run.
"""

def main():
    today = date.today()
    print(f"[INFO] Updating FF5 daily factors on {today}.")

    df = download_ff5_daily()

    key = upload_to_s3("factors", df, None, None, None)

    print(f"[OK] Factors update complete.")


if __name__ == "__main__":
    main()