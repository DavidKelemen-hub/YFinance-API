import json
import requests
import os
import time

JSON_FILE = "nyse_new_tickers.json"
OUTPUT_DIR = "nyse_logos"
TOKEN = "pk_WxmAqhBOQY2ioXO5HqBHQA"  # <-- replace with your token
os.makedirs(OUTPUT_DIR, exist_ok=True)

with open(JSON_FILE, "r", encoding="utf-8-sig") as f:
    symbols = [item["symbol"] for item in json.load(f)["symbols"]]

failed = []

for symbol in symbols:
    logo_path = f"{OUTPUT_DIR}/{symbol}.png"
    if os.path.exists(logo_path):
        print(f"  ⏭️  {symbol} already exists, skipping")
        continue

    url = f"https://img.logo.dev/ticker/{symbol}?token={TOKEN}&format=png&fallback=404"
    try:
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            with open(logo_path, "wb") as f:
                f.write(r.content)
            print(f"  ✅ {symbol}")
        else:
            print(f"  ❌ No logo for {symbol} (HTTP {r.status_code})")
            failed.append(symbol)
    except Exception as e:
        print(f"  ❌ {symbol} error: {e}")
        failed.append(symbol)

    time.sleep(0.2)

with open("logos_failed.txt", "w") as f:
    f.write("\n".join(failed))

print(f"\nDone. {len(failed)} symbols need manual follow-up → logos_failed.txt")