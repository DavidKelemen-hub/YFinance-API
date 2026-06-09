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
PERIOD = "max"
# ---------------------------

def to_dec2_or_none(x):
    if pd.isna(x):
        return None
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def to_int_or_none(x):
    if pd.isna(x):
        return None
    return int(x)

def get_stock_id(symbol: str):
    cursor.execute("SELECT StockID FROM dbo.Company WHERE Symbol = ?", symbol)
    r = cursor.fetchone()
    return r.StockID if r else None

BASE = os.path.abspath(os.path.dirname(__file__))
input_full_path = os.path.join(BASE, "nasdaq_tickers.json")
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

# ✅ MERGE: updates if exists, inserts if not
insert_sql = """
MERGE dbo.Earnings AS target
USING (SELECT ? AS StockID) AS source
ON target.StockID = source.StockID
WHEN MATCHED THEN
    UPDATE SET
        TrailingEPS       = ?,
        ForwardEPS        = ?,
        BookValue         = ?,
        FreeCashflow      = ?,
        EarningsGrowth    = ?,
        RevenueGrowth     = ?,
        SharesOutstanding = ?,
        TotalDebt         = ?,
        TotalCash         = ?,
        EBITDA            = ?,
        DividendRate      = ?,
        DividendYield     = ?,
        DebtToEquity      = ?,
        ReturnOnEquity    = ?,
        ReturnOnAssets    = ?,
        CurrentRatio      = ?,
        GrossMargins      = ?,
        OperatingMargins  = ?,
        Beta              = ?,
        Sector            = ?,
        RiskFreeRate      = ?,
        LatestUpdate      = ?
WHEN NOT MATCHED THEN
    INSERT (StockID, TrailingEPS, ForwardEPS, BookValue, FreeCashflow, EarningsGrowth, RevenueGrowth, SharesOutstanding, TotalDebt, TotalCash, EBITDA, DividendRate, DividendYield, DebtToEquity, ReturnOnEquity, ReturnOnAssets, CurrentRatio, GrossMargins, OperatingMargins, Beta, Sector, RiskFreeRate, LatestUpdate)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

tnx = yf.Ticker("^TNX")
risk_free_rate = tnx.info.get("regularMarketPrice")
totalLength = len(symbols)
currentIndex = 1

for symbol in symbols:
    print(f"Fetching {symbol}...")

    stock_id = get_stock_id(symbol)
    if stock_id is None:
        print(f"  ❌ Symbol {symbol} not found in Company table")
        currentIndex += 1
        continue

    ticker = yf.Ticker(symbol)
    info = ticker.info

    trailingEPS       = info.get("trailingEps")
    forwardEPS        = info.get("forwardEps")
    bookValue         = info.get("bookValue")
    freeCashflow      = info.get("freeCashflow")
    earningsGrowth    = info.get("earningsGrowth")
    revenueGrowth     = info.get("revenueGrowth")
    sharesOutstanding = info.get("sharesOutstanding")
    totalDebt         = info.get("totalDebt")
    totalCash         = info.get("totalCash")
    ebitda            = info.get("ebitda")
    dividendRate      = info.get("dividendRate")
    dividendYield     = info.get("dividendYield")
    debtToEquity      = info.get("debtToEquity")
    returnOnEquity    = info.get("returnOnEquity")
    returnOnAssets    = info.get("returnOnAssets")
    beta              = info.get("beta")
    currentRatio      = info.get("currentRatio")
    grossMargins      = info.get("grossMargins")
    operatingMargins  = info.get("operatingMargins")
    sector            = info.get("sector", "Unknown")
    quarter           = info.get("mostRecentQuarter")
    latestUpdate      = datetime.fromtimestamp(quarter, tz=timezone.utc).date() if quarter else None

    # Shared values used in both UPDATE and INSERT
    values = (
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
        to_dec2_or_none(dividendYield),
        to_dec2_or_none(debtToEquity),
        to_dec2_or_none(returnOnEquity),
        to_dec2_or_none(returnOnAssets),
        to_dec2_or_none(currentRatio),
        to_dec2_or_none(grossMargins),
        to_dec2_or_none(operatingMargins),
        to_dec2_or_none(beta),
        sector,
        to_dec2_or_none(risk_free_rate),
        latestUpdate,
    )

    # MERGE params: StockID (USING) + values (UPDATE) + StockID + values (INSERT)
    params = (stock_id,) + values + (stock_id,) + values

    try:
        cursor.execute(insert_sql, params)
        conn.commit()
        print(f"  ✅ Upserted {symbol} — {currentIndex}/{totalLength}, {totalLength - currentIndex} remaining")
    except Exception as e:
        conn.rollback()
        print(f"  ❌ Failed {symbol}: {e}")

    currentIndex += 1

cursor.close()
conn.close()
print("Done.")