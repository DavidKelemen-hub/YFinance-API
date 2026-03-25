import yfinance as yf
import pyodbc
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# ---------- CONFIG ----------
SERVER = "localhost"
DB_NAME = "StockData"
SYMBOL = "PFG"        # <<< CHANGE THIS
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

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DB_NAME};"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()
cursor.fast_executemany = True

# Get StockID
cursor.execute("SELECT StockID FROM dbo.Company WHERE Symbol = ?", SYMBOL)
row = cursor.fetchone()
if row is None:
    raise ValueError(f"Symbol {SYMBOL} not found in Company table")

stock_id = row.StockID
print(f"Processing {SYMBOL} (StockID={stock_id})")

# Fetch data
df = yf.Ticker(SYMBOL).history(period=PERIOD)

if df.empty:
    print("No data returned.")
    cursor.close()
    conn.close()
    raise SystemExit(0)

df = df.reset_index()
df["Date"] = pd.to_datetime(df["Date"]).dt.date

insert_sql = """
IF NOT EXISTS (
    SELECT 1 FROM dbo.DailyPrices
    WHERE StockID = ? AND TradeDate = ?
)
INSERT INTO dbo.DailyPrices
(StockID, TradeDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

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

print(f"Inserted (or already existed) {len(rows)} rows for {SYMBOL}")

cursor.close()
conn.close()
