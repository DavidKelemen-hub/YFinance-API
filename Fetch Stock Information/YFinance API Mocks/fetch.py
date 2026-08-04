import json
import yfinance as yf
import pyodbc
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import time


'''
['ADAC', 'AKAF', 'ANSS', 'BANFP', 'BEBE', 'BIII', 'BPAC', 'BRBI', 'CAPN', 'CCAQ', 'CCXI', 'CGCT', 'CHEC', 'CHPG', 'CTLT', 'CTRA', 'DAY', 'DFS', 'DGICB', 'DTSQ', 'EFTY', 'EMPG', 'FBYDP', 'FEDU', 'FFIC', 'FI', 'FONR', 'FSHP', 'GECCI', 'GJR', 'GJT', 'GPAC', 'GPAT', 'GSRF', 'HES', 'HOLX', 'HVMC', 'IGCB', 'INHD', 'IPG', 'IRAB', 'JATT', 'JDZG', 'JNPR', 'K', 'KCHV', 'KPET', 'LAFA', 'LAWR', 'LWAC', 'MAGH', 'MAMK', 'MCTA', 'MMC', 'MMTX', 'MRO', 'MUSE', 'NBRG', 'NEWTO', 'NMP', 'NUTR', 'NVA', 'OBA', 'OFSSO', 'OIM', 'ORIQ', 'OST', 'PACH', 'PAII', 'PARA', 'PC', 'PCAP', 'PLTS', 'PRHIZ', 'PTNM', 'QETA', 'QMMM', 'SAJ', 'SCPQ', 'SDM', 'SENEB', 'SRL', 'TMOS', 'UCFI', 'UYSC', 'WBA', 'WHLRD', 'WHLRL', 'WHLRP', 'WRK', 'WSTN', 'ZKPW']
'''
tickers = ['ADAC', 'AKAF', 'ANSS', 'BANFP', 'BEBE', 'BIII', 'BPAC', 'BRBI', 'CAPN', 'CCAQ', 'CCXI', 'CGCT', 'CHEC', 'CHPG', 'CTLT', 'CTRA', 'DAY', 'DFS', 'DGICB', 'DTSQ', 'EFTY', 'EMPG', 'FBYDP', 'FEDU', 'FFIC', 'FI', 'FONR', 'FSHP', 'GECCI', 'GJR', 'GJT', 'GPAC', 'GPAT', 'GSRF', 'HES', 'HOLX', 'HVMC', 'IGCB', 'INHD', 'IPG', 'IRAB', 'JATT', 'JDZG', 'JNPR', 'K', 'KCHV', 'KPET', 'LAFA', 'LAWR', 'LWAC', 'MAGH', 'MAMK', 'MCTA', 'MMC', 'MMTX', 'MRO', 'MUSE', 'NBRG', 'NEWTO', 'NMP', 'NUTR', 'NVA', 'OBA', 'OFSSO', 'OIM', 'ORIQ', 'OST', 'PACH', 'PAII', 'PARA', 'PC', 'PCAP', 'PLTS', 'PRHIZ', 'PTNM', 'QETA', 'QMMM', 'SAJ', 'SCPQ', 'SDM', 'SENEB', 'SRL', 'TMOS', 'UCFI', 'UYSC', 'WBA', 'WHLRD', 'WHLRL', 'WHLRP', 'WRK', 'WSTN', 'ZKPW']
# ---------- CONFIG ----------
JSON_FILE = "combined_tickers.json"
DB_NAME = "StockData"
SERVER = "localhost"
START_PERIOD = "max"
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
# ❌ Removed fast_executemany — incompatible with NULL decimals + IF NOT EXISTS

def get_stock_id(symbol: str):
    cursor.execute("SELECT StockID FROM dbo.Company WHERE Symbol = ?", symbol)
    r = cursor.fetchone()
    return r.StockID if r else None

# ✅ Clean MERGE statement — single param set, handles NULLs correctly
insert_sql = """
MERGE dbo.DailyPrices AS target
USING (SELECT ? AS StockID, CAST(? AS DATE) AS TradeDate) AS source
ON target.StockID = source.StockID AND target.TradeDate = source.TradeDate
WHEN NOT MATCHED THEN
    INSERT (StockID, TradeDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume)
    VALUES (?, ?, ?, ?, ?, ?, ?);
"""

start = time.time()
count = 1
totalLength = len(tickers)
failedTickers = []
for symbol in tickers:
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
        rows.append((
            stock_id, r["Date"],   # USING clause (duplicate check)
            stock_id, r["Date"],   # INSERT values
            to_dec4_or_none(r["Open"]),
            to_dec4_or_none(r["High"]),
            to_dec4_or_none(r["Low"]),
            to_dec4_or_none(r["Close"]),
            to_int_or_none(r["Volume"])
        ))

    cursor.executemany(insert_sql, rows)
    conn.commit()

    print(f"  ✅ Processed {len(rows)} rows for {symbol}")
    print(f"  {count} inserted, {totalLength - count} left.")
    count += 1
    

end = time.time()
print(f"Total time: {end - start:.1f}s")
print(f"Failed Tickers: {failedTickers}")

cursor.close()
conn.close()
print("Done.")
