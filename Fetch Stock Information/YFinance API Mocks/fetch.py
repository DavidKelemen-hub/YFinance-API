import yfinance as yf
import os
import json

# JSON_FILE = "sp100.json"

# # Load symbols from JSON
# BASE = os.path.abspath(os.path.dirname(__file__))
# input_full_path  = os.path.join(BASE, "sp100.json") 
# with open(input_full_path, "r", encoding="utf-8-sig") as f:
#     data = json.load(f)

# symbols = [item["symbol"] for item in data["symbols"]]

data = yf.Ticker("ZYME")
print(data.history(period='1mo'))
