import yfinance as yf
import pandas as pd
from fastapi import FastAPI
import pydantic
import json

app = FastAPI()

class News:
    def __init__(self, title, url, thumbnail):
        self.title = title
        self.url = url
        self.thumbnail = thumbnail

def get_ticker_news(symbol, newscount):
    news = []
    ticker = yf.Ticker(symbol).get_news(count=newscount, tab='news')
    for i in range(newscount):
        data = ticker[i]
        news.append({
            "title": data["content"]["title"],
            "url": data["content"]["canonicalUrl"]["url"],
            "thumbnail": data["content"]["thumbnail"]["resolutions"][1]["url"]
        })
    return news

@app.get("/")
async def root():
    ticker_news = get_ticker_news("MSFT",3)
    return {"message": ticker_news}