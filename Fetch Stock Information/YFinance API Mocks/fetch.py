import yfinance as yf
import os
import json

JSON_FILE = "sp100.json"

# Load symbols from JSON
BASE = os.path.abspath(os.path.dirname(__file__))
input_full_path  = os.path.join(BASE, "sp100.json") 
with open(input_full_path, "r", encoding="utf-8-sig") as f:
    data = json.load(f)

symbols = [item["symbol"] for item in data["symbols"]]

news = yf.Ticker("zbra").get_news(count=5)
print(news)
'''
for symbol in symbols:
    print(f"Fetching {symbol}...")
    ticker = yf.Ticker(symbol).get_recommendations()
    if ticker['strongSell'].any():
        print(f"Recommendations for {symbol}:\n {ticker}")
        '''