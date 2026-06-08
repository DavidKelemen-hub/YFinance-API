import yfinance as yf
import pandas as pd
from fastapi import FastAPI
import pydantic
import uvicorn
import json

app = FastAPI()

class News:
    def __init__(self, title, url, thumbnail):
        self.title = title
        self.url = url
        self.thumbnail = thumbnail

def get_ticker_news(symbol, newscount):
    news = []
    
    try:
        ticker = yf.Ticker(symbol).get_news(count=newscount, tab='news')
    except Exception:
        return news

    for i in range(min(newscount, len(ticker))):
        try:
            data = ticker[i]
            content = data.get("content") or {}
            canonical = content.get("canonicalUrl") or {}
            thumbnail = content.get("thumbnail") or {}

            news.append({
                "title": content.get("title", "None"),
                "url": canonical.get("url", "None"),
                "thumbnail": thumbnail.get("originalUrl", "None")
            })
        except Exception:
            news.append({
                "title": "None",
                "url": "None",
                "thumbnail": "None"
            })

    return news

@app.get("/news")
async def root(symbol: str, size: int):
    ticker_news = get_ticker_news(symbol,size)
    return {"newsfeed": ticker_news}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)