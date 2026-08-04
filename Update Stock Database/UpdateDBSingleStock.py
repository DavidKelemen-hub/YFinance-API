import yfinance as yf
import pyodbc
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

# ---------- CONFIG ----------
SERVER = "localhost"
DB_NAME = "StockData"
SYMBOL = "TIC"        # <<< CHANGE THIS
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
# ❌ fast_executemany removed — incompatible with NULL decimals + MERGE

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

# ✅ MERGE with UPDATE branch — overwrites existing rows so split-adjusted
#    prices from yfinance replace stale, un-adjusted values already in the DB.
insert_sql = """
MERGE dbo.DailyPrices AS target
USING (SELECT ? AS StockID, CAST(? AS DATE) AS TradeDate) AS source
ON target.StockID = source.StockID AND target.TradeDate = source.TradeDate
WHEN MATCHED THEN
    UPDATE SET
        OpenPrice  = ?,
        HighPrice  = ?,
        LowPrice   = ?,
        ClosePrice = ?,
        Volume     = ?
WHEN NOT MATCHED THEN
    INSERT (StockID, TradeDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume)
    VALUES (?, ?, ?, ?, ?, ?, ?);
"""

rows = []
for _, r in df.iterrows():
    open_p = to_dec4_or_none(r["Open"])
    high_p = to_dec4_or_none(r["High"])
    low_p = to_dec4_or_none(r["Low"])
    close_p = to_dec4_or_none(r["Close"])
    vol = to_int_or_none(r["Volume"])

    rows.append((
        stock_id, r["Date"],           # USING clause (match key)
        # WHEN MATCHED -> UPDATE values
        open_p, high_p, low_p, close_p, vol,
        # WHEN NOT MATCHED -> INSERT values
        stock_id, r["Date"],
        open_p, high_p, low_p, close_p, vol
    ))

cursor.executemany(insert_sql, rows)
conn.commit()

print(f"Inserted/updated {len(rows)} rows for {SYMBOL}")

cursor.close()
conn.close()