import time, feedparser
from .db import insert_news

FEEDS = [
  "https://www.bbc.com/sport/football/rss.xml",
  "https://www.espn.com/espn/rss/news",
]

def fetch_and_store_news(conn):
    for url in FEEDS:
        try:
            d = feedparser.parse(url)
            for e in d.entries[:50]:
                title = getattr(e, "title", "")
                link  = getattr(e, "link", "")
                published = getattr(e, "published_parsed", None)
                ts = int(time.mktime(published)*1000) if published else int(time.time()*1000)
                if title and link:
                    insert_news(conn, d.feed.title if hasattr(d,'feed') else url, link, title, ts)
        except Exception as e:
            print("[news] feed error:", url, e)
