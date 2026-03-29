import yfinance as yf
import pandas as pd
import pyodbc

### script to update median values ###

# ---------- CONFIG ----------
SERVER = "localhost"
DB_NAME = "StockData"
PERIOD = "max"
# ---------------------------

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DB_NAME};"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()
cursor.fast_executemany = True

### get list of sectors from DB ###
sector_sql_query = """
SELECT DISTINCT Sector FROM Earnings
WHERE Sector IS NOT NULL AND Sector != 'Unknown'
"""

### calculate median pe for each sector ###
median_pe_query = """
EXEC GetSectorMedianPE ?
"""

### calculate median ev/ebitda for each sector ###
median_ev_ebitda_query = """
EXEC GetSectorMedianEV_EBITDA ?
"""

### sql query to insert median values into db ###
insert_median_sql = """
MERGE dbo.SectorMedians AS target
USING (VALUES (?, ?, ?)) AS source (Sector, Median_EV_EBITDA, Median_PE)
ON target.Sector = source.Sector
WHEN MATCHED THEN
    UPDATE SET Median_EV_EBITDA = source.Median_EV_EBITDA, Median_PE = source.Median_PE
WHEN NOT MATCHED THEN
    INSERT (Sector, Median_EV_EBITDA, Median_PE)
    VALUES (source.Sector, source.Median_EV_EBITDA, source.Median_PE);
"""

cursor.execute(sector_sql_query)
sectors = cursor.fetchall()

for sector in sectors:
    sector_name = sector[0]
    cursor.execute(median_pe_query,sector_name)
    median_pe = cursor.fetchone()[0]
    cursor.execute(median_ev_ebitda_query,sector_name)
    median_ev_ebitda = cursor.fetchone()[0]
    cursor.execute(insert_median_sql,sector_name,median_ev_ebitda, median_pe)
    conn.commit()
    print(f"Sector: {sector_name}, Median PE: {median_pe}, Median EV/EBITDA: {median_ev_ebitda}")

