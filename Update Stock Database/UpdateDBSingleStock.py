import yfinance as yf
import pyodbc
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

# ---------- CONFIG ----------
SERVER = "localhost"
DB_NAME = "StockData"
SYMBOL = "ADTX"        # <<< CHANGE THIS
PERIOD = "max"
# ---------------------------

def to_dec4_or_none(x):
    if pd.isna(x):
        return None
    try:
        d = Decimal(str(x)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        # decimal(18,4) max is 99999999999999.9999
        if d > Decimal("99999999999999.9999") or d < Decimal("-99999999999999.9999"):
            return None
        return d
    except (InvalidOperation, ValueError):
        return None

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
        to_dec4_or_none(r["Open"]),
        to_dec4_or_none(r["High"]),
        to_dec4_or_none(r["Low"]),
        to_dec4_or_none(r["Close"]),
        to_int_or_none(r["Volume"])
    ))

cursor.executemany(insert_sql, rows)
conn.commit()

print(f"Inserted (or already existed) {len(rows)} rows for {SYMBOL}")

cursor.close()
conn.close()
