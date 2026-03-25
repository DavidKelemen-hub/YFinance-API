import json
import yfinance as yf
import pyodbc
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# ---------- CONFIG ----------
JSON_FILE = "sp100.json"
DB_NAME = "StockData"
SERVER = "localhost"
START_PERIOD = "max"
# ----------------------------

def to_dec2_or_none(x):
    """Return Decimal rounded to 2dp or None for NaN/None (inserts NULL)."""
    if pd.isna(x):
        return None
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def to_int_or_none(x):
    """Return int or None for NaN/None (inserts NULL)."""
    if pd.isna(x):
        return None
    return int(x)

# Connect to SQL Server
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DB_NAME};"
    f"Trusted_Connection=yes;"
)
cursor = conn.cursor()
cursor.fast_executemany = True

# Load symbols from JSON
with open(JSON_FILE, "r", encoding="utf-8-sig") as f:
    data = json.load(f)

symbols = [item["symbol"] for item in data["symbols"]]

# Helper: get StockID from DB
def get_stock_id(symbol: str):
    cursor.execute("SELECT StockID FROM dbo.Company WHERE Symbol = ?", symbol)
    r = cursor.fetchone()
    return r.StockID if r else None

# Insert SQL (idempotent)
insert_sql = """
IF NOT EXISTS (
    SELECT 1 FROM dbo.DailyPrices
    WHERE StockID = ? AND TradeDate = ?
)
INSERT INTO dbo.DailyPrices
(StockID, TradeDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

# Loop through all symbols
for symbol in symbols:
    print(f"Fetching {symbol}...")

    stock_id = get_stock_id(symbol)
    if stock_id is None:
        print(f"  ❌ Symbol {symbol} not found in Company table")
        continue

    df = yf.Ticker(symbol).history(period=START_PERIOD)

    if df.empty:
        print(f"  ⚠️ No data for {symbol}")
        continue

    df = df.reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.date

    rows = []
    for _, r in df.iterrows():
        rows.append((
            stock_id,
            r["Date"],
            stock_id,
            r["Date"],
            to_dec2_or_none(r["Open"]),
            to_dec2_or_none(r["High"]),
            to_dec2_or_none(r["Low"]),
            to_dec2_or_none(r["Close"]),
            to_int_or_none(r["Volume"])
        ))

    cursor.executemany(insert_sql, rows)
    conn.commit()

    print(f"  ✅ Processed {len(rows)} rows for {symbol}")

cursor.close()
conn.close()
print("Done.")
