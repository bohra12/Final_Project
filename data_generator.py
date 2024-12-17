import sqlite3
import requests
from datetime import datetime
import yfinance as yf

DB_NAME = "stock_data.db"
MARKET_AUX_KEY = "EqyXY77oTkKwEHDoA06ieJoTER0CMu6X15QAhy0d"
FINNHUB_KEY = "ctfne0pr01qi0nfdon4gctfne0pr01qi0nfdon50"
FMP_KEY = "6MwrkQYWb9lkTrkJSTm3budgQiAo2Or6" 


def setup_database():
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
            date INTEGER,
            open REAL,
            close REAL,
            high REAL,
            low REAL,
            FOREIGN KEY(stock_id) REFERENCES Stocks(stock_id)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS Dividends (
            dividend_id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_id INTEGER,
            date INTEGER,
            dividend INTEGER,
            FOREIGN KEY(stock_id) REFERENCES Stocks(stock_id)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS InsiderTransactions (
            transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_id INTEGER,
            filing_date INTEGER,
            transaction_date INTEGER,
            transaction_price INTEGER,
            share INTEGER,
            change INTEGER,
            FOREIGN KEY(stock_id) REFERENCES Stocks(stock_id)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS News (
            news_id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT,
            title TEXT
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS Sentiments (
            sentiment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            news_id INTEGER,
            stock_id INTEGER,
            sentiment REAL,
            FOREIGN KEY(news_id) REFERENCES News(news_id),
            FOREIGN KEY(stock_id) REFERENCES Stocks(stock_id)
        )
    ''')

    conn.commit()
    conn.close()


def fetch_stock_prices_yahoo(symbol, conn, max_rows):
    try:
        stock = yf.Ticker(symbol)
        data = stock.history(period="6mo")
        stock_prices = []
        count = 0

        for date, row in data.iterrows():
            date_int = int(datetime.strptime(str(date.date()), "%Y-%m-%d").strftime("%Y%m%d"))

            cur = conn.cursor()
            cur.execute("""
                SELECT 1 FROM StockPrices WHERE date = ? AND stock_id = (
                    SELECT stock_id FROM Stocks WHERE ticker = ?
                )
            """, (date_int, symbol))
            if cur.fetchone():
                continue

            stock_prices.append({
                "date": date_int,
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

def fetch_insider_transactions_finnhub(symbol, conn, max_rows):
    url = "https://finnhub.io/api/v1/stock/insider-transactions"
    params = {"symbol": symbol, "token": FINNHUB_KEY}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO Stocks (ticker) VALUES (?)", (symbol,))
        conn.commit()

        cur.execute("SELECT stock_id FROM Stocks WHERE ticker = ?", (symbol,))
        stock_id = cur.fetchone()[0]

        count = 0
        for record in data.get("data", []):
            filing_date = record.get("filingDate")
            transaction_date = record.get("transactionDate")
            transaction_price = record.get("transactionPrice", 0)
            share = record.get("share", 0)
            change = record.get("change", 0)
            if not filing_date or not transaction_date:
                print(f"Skipping invalid record for {symbol}: {record}")
                continue
            filing_date_int = int(datetime.strptime(filing_date, "%Y-%m-%d").strftime("%Y%m%d"))
            transaction_date_int = int(datetime.strptime(transaction_date, "%Y-%m-%d").strftime("%Y%m%d"))
            transaction_price_int = int(float(transaction_price) * 100)

            cur.execute("""
                INSERT INTO InsiderTransactions (stock_id, filing_date, transaction_date, 
                transaction_price, share, change)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (stock_id, filing_date_int, transaction_date_int, transaction_price_int, share, change))

            count += 1
            if count >= max_rows:
                break

        conn.commit()
        print(f"Fetched and stored {count} insider transactions for {symbol}.")

    except Exception as e:
        print(f"Error fetching insider transactions for {symbol}: {e}")

        

def fetch_dividends_fmp(symbol, conn, max_rows=10):
    base_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/{symbol}"
    params = {"apikey": FMP_KEY}

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        historical_dividends = data.get("historical", [])
        if not historical_dividends:
            print(f"No dividend data found for {symbol}.")
            return []

        cur = conn.cursor()
        # Ensure the stock exists in Stocks table
        cur.execute("INSERT OR IGNORE INTO Stocks (ticker) VALUES (?)", (symbol,))
        conn.commit()

        # Fetch stock_id for the symbol
        cur.execute("SELECT stock_id FROM Stocks WHERE ticker = ?", (symbol,))
        stock_id = cur.fetchone()[0]

        count = 0
        for record in historical_dividends:
            # Extract fields
            date = record.get("date")
            dividend = record.get("dividend")

            # Check for missing fields
            if not date or dividend is None:
                print(f"Skipping invalid dividend record for {symbol}: {record}")
                continue

            # Convert date and dividend to integers
            date_int = int(datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d"))
            dividend_int = int(float(dividend) * 100)  # Convert dividend to cents

            # Insert data into Dividends table (duplicates allowed)
            cur.execute("""
                INSERT INTO Dividends (stock_id, date, dividend)
                VALUES (?, ?, ?)
            """, (stock_id, date_int, dividend_int))

            count += 1
            if count >= max_rows:
                break

        conn.commit()
        print(f"Fetched and stored {count} dividend entries for {symbol}.")

    except Exception as e:
        print(f"Error fetching dividends for {symbol}: {e}")


def fetch_news_marketaux(symbol, conn, max_rows):
    cur = conn.cursor()
    url = "https://api.marketaux.com/v1/news/all"
    page = 1  
    fetched_rows = 0

    try:
        cur.execute("INSERT OR IGNORE INTO Stocks (ticker) VALUES (?)", (symbol,))
        conn.commit()

        cur.execute("SELECT stock_id FROM Stocks WHERE ticker = ?", (symbol,))
        stock_id = cur.fetchone()[0]

        while fetched_rows < max_rows:
            params = {
                "api_token": MARKET_AUX_KEY,
                "symbols": symbol,
                "limit": max_rows - fetched_rows,  
                "language": "en",
                "filter_entities": True,
                "page": page
            }

            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            articles = data.get("data", [])
            if not articles:  
                print(f"No more articles found for {symbol}.")
                break

            sentiment_data = []

            for article in articles:
                title = article.get("title", "No Title")
                news_url = article.get("url", "No URL")

                cur.execute("SELECT 1 FROM News WHERE url = ?", (news_url,))
                if cur.fetchone():
                    print(f"Duplicate news entry found: {news_url}. Skipping.")
                    continue

                cur.execute("""
                    INSERT INTO News (url, title)
                    VALUES (?, ?)
                """, (news_url, title))
                news_id = cur.lastrowid
                entities = article.get("entities", [])
                sentiment_scores = [entity.get("sentiment_score", 0.0) for entity in entities if "sentiment_score" in entity]
                average_sentiment = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0.0
                sentiment_data.append((news_id, stock_id, average_sentiment))
                fetched_rows += 1
                if fetched_rows >= max_rows:
                    break  

            if sentiment_data:
                cur.executemany("""
                    INSERT INTO Sentiments (news_id, stock_id, sentiment)
                    VALUES (?, ?, ?)
                """, sentiment_data)

            conn.commit()
            print(f"Fetched and stored {len(sentiment_data)} news entries for {symbol}.")
            page += 1  

    except requests.exceptions.RequestException as e:
        print(f"Error fetching news for {symbol}: {e}")



def store_data_in_db(conn, symbol, stock_prices):
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO Stocks (ticker) VALUES (?)", (symbol,))
    stock_id = cur.execute("SELECT stock_id FROM Stocks WHERE ticker = ?", (symbol,)).fetchone()[0]

    for price in stock_prices:
        cur.execute('''
            INSERT OR IGNORE INTO StockPrices (stock_id, date, open, close, high, low)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (stock_id, price["date"], price["open"], price["close"], price["high"], price["low"]))

    conn.commit()


def main():
    setup_database()
    symbols = ["AAPL", "GOOGL", "MSFT", "TSLA"]
    max_rows = 6

    conn = sqlite3.connect(DB_NAME)

    for symbol in symbols:
        print(f"Fetching data for {symbol}...")

        stock_prices = fetch_stock_prices_yahoo(symbol, conn, max_rows)
        fetch_news_marketaux(symbol, conn, max_rows)
        store_data_in_db(conn, symbol, stock_prices)
        dividends = fetch_dividends_fmp(symbol, conn, max_rows)
        fetch_insider_transactions_finnhub(symbol, conn, max_rows)

        print(f"Data for {symbol} stored successfully.\n")

    conn.close()


if __name__ == "__main__":
    main()
