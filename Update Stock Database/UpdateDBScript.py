import json
import yfinance as yf
import pyodbc
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import time

# ---------- CONFIG ----------
JSON_FILE = "combined_tickers.json"
DB_NAME = "StockData"
SERVER = "localhost"
START_PERIOD = "1y"
# ----------------------------

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
    if pd.isna(x):
        return None
    return int(x)

conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DB_NAME};"
    f"Trusted_Connection=yes;"
)
cursor = conn.cursor()
# ❌ fast_executemany still off — incompatible with NULL decimals + MERGE

with open(JSON_FILE, "r", encoding="utf-8-sig") as f:
    data = json.load(f)

symbols = []
for item in data["symbols"]:
    try:
        symbols.append(item["symbol"])
    except KeyError:
        print(item)

def get_stock_id(symbol: str):
    cursor.execute("SELECT StockID FROM dbo.Company WHERE Symbol = ?", symbol)
    r = cursor.fetchone()
    return r.StockID if r else None

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

start = time.time()
count = 1
totalLength = len(symbols)
failedTickers = []
for symbol in symbols:
    print(f"Fetching {symbol}...")

    stock_id = get_stock_id(symbol)
    if stock_id is None:
        print(f"  ❌ Symbol {symbol} not found in Company table")
        failedTickers.append(symbol)
        continue

    df = yf.Ticker(symbol).history(period=START_PERIOD)

    if df.empty:
        print(f"  ⚠️ No data for {symbol}")
        failedTickers.append(symbol)
        continue

    df = df.reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.date

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

    print(f"  ✅ Processed {len(rows)} rows for {symbol}")
    print(f"  {count} inserted/updated, {totalLength - count} left.")
    count += 1


end = time.time()
print(f"Total time: {end - start:.1f}s")
print(f"Failed Tickers: {failedTickers}")

cursor.close()
conn.close()
print("Done.")