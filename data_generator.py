import sqlite3
import requests
from datetime import datetime
import yfinance as yf

DB_NAME = "stock_data.db"

MARKET_AUX_KEY = "la5KwzB2gLWX9Y8fKVsrAEYBEFEC6AnJyiUo7WLm"
FINNHUB_KEY = "ctfne0pr01qi0nfdon4gctfne0pr01qi0nfdon50"
OPENFIGI_KEY = "93bca677-feba-4a77-b7a2-b5a0894b822c"


def setup_database():
    """Set up the SQLite database with required tables."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS Stocks (
            stock_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT UNIQUE NOT NULL
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS StockPrices (
            price_id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_id INTEGER,
            date TEXT,
            open REAL,
            close REAL,
            high REAL,
            low REAL,
            FOREIGN KEY(stock_id) REFERENCES Stocks(stock_id),
            UNIQUE(stock_id, date)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS Dividends (
            dividend_id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_id INTEGER,
            date TEXT,
            dividend REAL,
            FOREIGN KEY(stock_id) REFERENCES Stocks(stock_id),
            UNIQUE(stock_id, date)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS InsiderTransactions (
            transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_id INTEGER,
            name TEXT,
            share INTEGER,
            change INTEGER,
            filing_date TEXT,
            transaction_date TEXT,
            transaction_code TEXT,
            transaction_price REAL,
            FOREIGN KEY(stock_id) REFERENCES Stocks(stock_id),
            UNIQUE(stock_id, filing_date, transaction_date, transaction_code)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS News (
            news_id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_id INTEGER,
            title TEXT,
            published_date TEXT,
            sentiment REAL,
            url TEXT,
            topics TEXT,
            FOREIGN KEY(stock_id) REFERENCES Stocks(stock_id),
            UNIQUE(stock_id, published_date)
        )
    ''')

    conn.commit()
    conn.close()


def fetch_stock_prices_yahoo(symbol, conn, max_rows=7):
    """Fetch historical stock prices using Yahoo Finance."""
    try:
        stock = yf.Ticker(symbol)
        data = stock.history(period="6mo")
        stock_prices = []
        count = 0

        for date, row in data.iterrows():
            cur = conn.cursor()
            cur.execute("""
                SELECT 1 FROM StockPrices WHERE date = ? AND stock_id = (
                    SELECT stock_id FROM Stocks WHERE ticker = ?
                )
            """, (date.strftime("%Y-%m-%d"), symbol))
            if cur.fetchone():
                continue

            stock_prices.append({
                "date": date.strftime("%Y-%m-%d"),
                "open": row["Open"],
                "close": row["Close"],
                "high": row["High"],
                "low": row["Low"]
            })

            count += 1
            if count >= max_rows:
                break

        print(f"Fetched {len(stock_prices)} stock price entries for {symbol}.")
        return stock_prices
    except Exception as e:
        print(f"Error fetching stock prices for {symbol}: {e}")
        return []


def fetch_dividends_yahoo(symbol, conn, max_rows=25):
    """Fetch dividend history for a given symbol using Yahoo Finance."""
    try:
        stock = yf.Ticker(symbol)
        dividends = stock.dividends
        dividend_data = []
        count = 0

        for date, dividend in dividends.items():
            date_str = date.strftime("%Y-%m-%d")
            cur = conn.cursor()
            cur.execute("""
                SELECT 1 FROM Dividends WHERE date = ? AND stock_id = (
                    SELECT stock_id FROM Stocks WHERE ticker = ?
                )
            """, (date_str, symbol))
            if cur.fetchone():
                continue

            dividend_data.append({
                "date": date_str,
                "dividend": float(dividend)
            })

            count += 1
            if count >= max_rows:
                break

        print(f"Fetched {len(dividend_data)} dividend entries for {symbol}.")
        return dividend_data
    except Exception as e:
        print(f"Error fetching dividends for {symbol}: {e}")
        return []

def fetch_insider_transactions_finnhub(symbol, conn, max_rows=7):
    """Fetch insider transaction data using Finnhub API."""
    url = f"https://finnhub.io/api/v1/stock/insider-transactions"
    params = {"symbol": symbol, "token": FINNHUB_KEY, "limit": max_rows}
    response = requests.get(url, params=params)
    data = response.json()
    insider_data = []
    count = 0

    for record in data["data"]:
        if count >= max_rows:
            break

        filing_date = record.get("filingDate", "Unknown")
        cur = conn.cursor()
        cur.execute("""
            SELECT 1 FROM InsiderTransactions WHERE filing_date = ? AND stock_id = (
                SELECT stock_id FROM Stocks WHERE ticker = ?
            )
        """, (filing_date, symbol))
        if cur.fetchone():
            continue

        insider_data.append({
            "name": record.get("name", "Unknown"),
            "share": int(record.get("share", 0)),
            "change": int(record.get("change", 0)),
            "filing_date": filing_date,
            "transaction_date": record.get("transactionDate", "Unknown"),
            "transaction_code": record.get("transactionCode", "Unknown"),
            "transaction_price": float(record.get("transactionPrice", 0.0))
        })

        count += 1

    print(f"Fetched {len(insider_data)} insider transactions for {symbol}.")
    return insider_data


def fetch_news_marketaux(symbol, conn):
    """Fetches 25 news entries per run using MarketAux API."""
    cur = conn.cursor()
    cur.execute("""
        SELECT MAX(published_date) FROM News
        JOIN Stocks ON News.stock_id = Stocks.stock_id
        WHERE ticker = ?
    """, (symbol,))
    last_published_date = cur.fetchone()[0]

    print(f"Last news published date for {symbol}: {last_published_date}")

    url = "https://api.marketaux.com/v1/news/all"
    params = {
        "api_token": MARKET_AUX_KEY,
        "symbols": symbol,
        "limit": 100,  
        "language": "en",
        "filter_entities": True
    }

    news_data = []
    page = 1
    while len(news_data) < 6:  
        params["page"] = page
        response = requests.get(url, params=params)
        data = response.json()

        if "error" in data:
            error_message = data["error"].get("message", "Unknown error")
            print(f"Error fetching news for {symbol}: {error_message}")
            break

        articles = data.get("data", [])
        if not articles:
            break  

        for article in articles:
            published_date = article.get("published_at", "Unknown")
            cur.execute("""
                SELECT 1 FROM News WHERE published_date = ? AND stock_id = (
                    SELECT stock_id FROM Stocks WHERE ticker = ?
                )
            """, (published_date, symbol))
            if cur.fetchone():
                continue

            entities = article.get("entities", [])
            sentiment_scores = [entity.get("sentiment_score", 0.0) for entity in entities if "sentiment_score" in entity]
            average_sentiment = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0.0
            entity_symbols = [entity.get("symbol", "Unknown") for entity in entities]

            news_data.append({
                "title": article.get("title", "No Title"),
                "published_date": published_date,
                "sentiment": average_sentiment,
                "url": article.get("url", "No URL"),
                "topics": ", ".join(entity_symbols) if entity_symbols else "No Topics"
            })

            if len(news_data) >= 25:  
                break

        page += 1  

    print(f"Fetched {len(news_data)} news entries for {symbol}.")
    return news_data

def store_data_in_db(conn, symbol, stock_prices, dividends, insider_transactions, news_data):
    """Store the fetched data into the database."""
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO Stocks (ticker) VALUES (?)", (symbol,))
    stock_id = cur.execute("SELECT stock_id FROM Stocks WHERE ticker = ?", (symbol,)).fetchone()[0]

    for price in stock_prices:
        cur.execute('''
            INSERT OR IGNORE INTO StockPrices (stock_id, date, open, close, high, low)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (stock_id, price["date"], price["open"], price["close"], price["high"], price["low"]))

    for dividend in dividends:
        cur.execute('''
            INSERT OR IGNORE INTO Dividends (stock_id, date, dividend)
            VALUES (?, ?, ?)
        ''', (stock_id, dividend["date"], dividend["dividend"]))

    for transaction in insider_transactions:
        cur.execute('''
            INSERT OR IGNORE INTO InsiderTransactions (stock_id, name, share, change, filing_date, transaction_date, transaction_code, transaction_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (stock_id, transaction["name"], transaction["share"], transaction["change"],
              transaction["filing_date"], transaction["transaction_date"], transaction["transaction_code"],
              transaction["transaction_price"]))

    for news in news_data:
        cur.execute('''
            INSERT OR IGNORE INTO News (stock_id, title, published_date, sentiment, url, topics)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (stock_id, news["title"], news["published_date"], news["sentiment"],
              news["url"], news["topics"]))

    conn.commit()


def main():
    setup_database()
    symbols = ["AAPL", "GOOGL", "MSFT", "TSLA"]
    max_rows = 25

    conn = sqlite3.connect(DB_NAME)

    for symbol in symbols:
        print(f"Fetching data for {symbol}...")

        stock_prices = fetch_stock_prices_yahoo(symbol, conn)
        dividends = fetch_dividends_yahoo(symbol, conn, max_rows)
        insider_transactions = fetch_insider_transactions_finnhub(symbol, conn)
        news_data = fetch_news_marketaux(symbol, conn)

        store_data_in_db(conn, symbol, stock_prices, dividends, insider_transactions, news_data)
        print(f"Data for {symbol} stored successfully.\n")

    conn.close()


if __name__ == "__main__":
    main()
