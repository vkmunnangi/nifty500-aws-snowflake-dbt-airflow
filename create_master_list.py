"""
create_master_list.py

Downloads the NIFTY 500 constituents CSV from NSE India,
appends '.NS' to each symbol for yfinance compatibility,
and saves a trimmed master list locally.
"""

import requests
import pandas as pd
from io import StringIO


NSE_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def download_nifty500_master() -> pd.DataFrame:
    """Download the NIFTY 500 CSV from NSE and return a cleaned DataFrame."""
    print("📥 Downloading NIFTY 500 master list from NSE India...")

    response = requests.get(NSE_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    df = pd.read_csv(StringIO(response.text))

    # Append .NS suffix so symbols work with yfinance
    df["Symbol"] = df["Symbol"].str.strip() + ".NS"

    # Keep only the columns we need
    df = df[["Symbol", "Company Name"]]

    print(f"✅ Parsed {len(df)} symbols from NIFTY 500 list.")
    return df


def main() -> None:
    df = download_nifty500_master()

    output_path = "nifty500_master.csv"
    df.to_csv(output_path, index=False)
    print(f"💾 Master list saved to {output_path}")


if __name__ == "__main__":
    main()
