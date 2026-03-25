from cmath import nan
import os

import yfinance as yf
import pyodbc
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timezone
import json

# ---------- CONFIG ----------
JSON_FILE = "sp100.json"
SERVER = "localhost"
DB_NAME = "StockData"
SYMBOL = "MSFT"        # <<< CHANGE THIS
PERIOD = "max"
# ---------------------------
def to_dec2_or_none(x):
    """Return Decimal rounded to 2dp or None for NaN/None (inserts NULL)."""
    if pd.isna(x):
        return None
    # str() avoids float binary artifacts; quantize enforces (10,2)-friendly values
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def to_int_or_none(x):
    """Return int or None for NaN/None (inserts NULL)."""
    if pd.isna(x):
        return None
    return int(x)

# Helper: get StockID from DB
def get_stock_id(symbol: str):
    cursor.execute("SELECT StockID FROM dbo.Company WHERE Symbol = ?", symbol)
    r = cursor.fetchone()
    return r.StockID if r else None

# Load symbols from JSON
BASE = os.path.abspath(os.path.dirname(__file__))
input_full_path  = os.path.join(BASE, "sp100.json") 
with open(input_full_path, "r", encoding="utf-8-sig") as f:
    data = json.load(f)

symbols = [item["symbol"] for item in data["symbols"]]

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DB_NAME};"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()
cursor.fast_executemany = True

insert_sql = """
IF NOT EXISTS (
    SELECT 1 FROM dbo.Earnings
    WHERE StockID = ?
)
INSERT INTO dbo.Earnings
(StockID, TrailingEPS, ForwardEPS, BookValue, FreeCashflow, EarningsGrowth, RevenueGrowth, SharesOutstanding, TotalDebt, TotalCash, EBITDA, DividendRate, DebtToEquity, ReturnOnEquity, ReturnOnAssets, CurrentRatio, GrossMargins, OperatingMargins, Sector, LatestUpdate)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

for symbol in symbols:
    print(f"Fetching {symbol}...")

    stock_id = get_stock_id(symbol)
    if stock_id is None:
        print(f"  ❌ Symbol {symbol} not found in Company table")
        continue

    ticker = yf.Ticker(symbol)
    info = ticker.info
    #print(info)
    ######### values to write to DB - only quarterly#########
    trailingEPS = info.get("trailingEps")  # Some stocks may not have this field]
    forwardEPS = info.get("forwardEps")  # Some stocks may not have this field]
    bookValue = info.get("bookValue")  # Some stocks may not have this field]
    freeCashflow = info.get("freeCashflow")  # Some stocks may not have this field]
    earningsGrowth = info.get("earningsGrowth")  # Some stocks may not have this field]
    revenueGrowth = info.get("revenueGrowth")  # Some stocks may not have this field]
    sharesOutstanding = info.get("sharesOutstanding")  # Some stocks may not have this field]
    totalDebt = info.get("totalDebt")  # Some stocks may not have this field
    totalCash = info.get("totalCash")  # Some stocks may not have this field
    ebitda = info.get("ebitda")  # Some stocks may not have this field
    trailingPE = info.get("trailingPE")  # Some stocks may not have this field
    #dividendRate = info["dividendRate"]
    dividendRate = info.get("dividendRate")  # Some stocks may not have this field
    dividendYield = info.get("dividendYield")  # Some stocks may not have this field
    debtToEquity = info.get("debtToEquity")  # Some stocks may not have this field
    returnOnEquity = info.get("returnOnEquity")  # Some stocks may not have this field
    returnOnAssets = info.get("returnOnAssets")  # Some stocks may not have this field
    currentRatio = info.get("currentRatio")  # Some stocks may not have this field
    grossMargins = info.get("grossMargins")  # Some stocks may not have this field
    operatingMargins = info.get("operatingMargins")  # Some stocks may not have this field
    sector = info.get("sector", "Unknown")  # Default to "Unknown" if sector is missing
    quarter = ticker.info.get("mostRecentQuarter")  
    if quarter:
        latestUpdate = datetime.fromtimestamp(quarter, tz=timezone.utc).date()
    else:
        latestUpdate = None# Some stocks may not have this field, default to end of 2025

    rows=[stock_id,
        stock_id, 
      to_dec2_or_none(trailingEPS), 
      to_dec2_or_none(forwardEPS), 
      to_dec2_or_none(bookValue), 
      to_dec2_or_none(freeCashflow), 
      to_dec2_or_none(earningsGrowth), 
      to_dec2_or_none(revenueGrowth), 
      to_int_or_none(sharesOutstanding), 
      to_dec2_or_none(totalDebt), 
      to_dec2_or_none(totalCash), 
      to_dec2_or_none(ebitda), 
      to_dec2_or_none(dividendRate), 
      to_dec2_or_none(debtToEquity), 
      to_dec2_or_none(returnOnEquity), 
      to_dec2_or_none(returnOnAssets), 
      to_dec2_or_none(currentRatio), 
      to_dec2_or_none(grossMargins), 
      to_dec2_or_none(operatingMargins), 
      sector, 
      latestUpdate]

    cursor.execute(insert_sql, tuple(rows))
    conn.commit()
    print(f"Inserted (or already existed) {len(rows)} rows for {symbol}")
#endfor

cursor.close()
conn.close()