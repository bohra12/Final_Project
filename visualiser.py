import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

DB_NAME = "stock_data.db"

def fetch_data_from_db():
    """Fetch all data from the SQLite database and return as pandas DataFrames."""
    conn = sqlite3.connect(DB_NAME)
    stocks = pd.read_sql_query("SELECT * FROM Stocks", conn)
    stock_prices = pd.read_sql_query("SELECT * FROM StockPrices", conn)
    dividends = pd.read_sql_query("SELECT * FROM Dividends", conn)
    insider_transactions = pd.read_sql_query("SELECT * FROM InsiderTransactions", conn)
    news = pd.read_sql_query("SELECT * FROM News", conn)

    conn.close()

    return stocks, stock_prices, dividends, insider_transactions, news


def calculate_stock_price_summary(stock_prices, stocks):
    """Summarize stock prices for each stock with min, max, and average prices."""
    price_summary = stock_prices.groupby("stock_id")[["open", "close", "high", "low"]].agg(
        min_price=("low", "min"),
        max_price=("high", "max"),
        avg_price=("close", "mean")
    ).reset_index()
    price_summary = price_summary.merge(stocks, on="stock_id")
    return price_summary[["ticker", "min_price", "max_price", "avg_price"]]


def calculate_dividend_yield(dividends, stock_prices, stocks):
    """Calculate dividend yield as a percentage, including stocks with no dividends."""
    latest_prices = stock_prices.groupby("stock_id")["close"].last().reset_index()
    dividend_totals = dividends.groupby("stock_id")["dividend"].sum().reset_index()

    dividend_yield = latest_prices.merge(dividend_totals, on="stock_id", how="left").fillna({"dividend": 0})
    dividend_yield["yield (%)"] = (dividend_yield["dividend"] / dividend_yield["close"]) * 100
    dividend_yield = dividend_yield.merge(stocks, on="stock_id")
    return dividend_yield[["ticker", "dividend", "yield (%)"]]


def calculate_news_sentiment(news, stocks):
    """Analyze average sentiment from news data."""
    sentiment_summary = news.groupby("stock_id")["sentiment"].mean().reset_index()
    sentiment_summary = sentiment_summary.merge(stocks, on="stock_id")
    return sentiment_summary[["ticker", "sentiment"]]


def summarize_insider_transactions(insider_transactions, stocks):
    """Summarize total shares bought/sold by insiders."""
    insider_summary = insider_transactions.groupby("stock_id")["change"].sum().reset_index()
    insider_summary = insider_summary.merge(stocks, on="stock_id")
    return insider_summary[["ticker", "change"]]


def visualize_data(df, title, x_col, y_col, xlabel, ylabel, color="skyblue", rotation=45):
    """Reusable function for bar charts."""
    plt.figure(figsize=(10, 5))
    plt.bar(df[x_col], df[y_col], color=color)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=rotation, ha="right")
    plt.grid(axis="y")
    plt.show()


def visualize_stock_prices(stock_prices, stocks):
    """Generate a line plot of stock prices over time for each stock."""
    stock_prices["date"] = pd.to_datetime(stock_prices["date"])
    stock_prices = stock_prices.merge(stocks, left_on="stock_id", right_on="stock_id")

    plt.figure(figsize=(12, 6))
    for ticker, group in stock_prices.groupby("ticker"):
        plt.plot(group["date"], group["close"], label=ticker)

    plt.title("Stock Prices Over Time")
    plt.xlabel("Date")
    plt.ylabel("Close Price")
    plt.legend()
    plt.grid()
    plt.show()


def visualize_news_topics(news, stocks):
    """Generate a topic frequency visualization."""
    news = news.merge(stocks, left_on="stock_id", right_on="stock_id")
    topics = news["topics"].str.split(", ").explode().value_counts().head(10)

    plt.figure(figsize=(12, 6))
    topics.plot(kind="bar", color="lightgreen")
    plt.title("Most Common News Topics (Top 10)")
    plt.xlabel("Topics")
    plt.ylabel("Frequency")
    plt.xticks(rotation=45, ha="right")
    plt.grid(axis="y")
    plt.show()


def main():
    stocks, stock_prices, dividends, insider_transactions, news = fetch_data_from_db()
    price_summary = calculate_stock_price_summary(stock_prices, stocks)
    print("\nStock Price Summary:")
    print(price_summary)
    visualize_data(price_summary, "Stock Price Summary", "ticker", "avg_price", "Ticker", "Average Price", color="lightblue")

    dividend_yield = calculate_dividend_yield(dividends, stock_prices, stocks)
    print("\nDividend Yield (including stocks with no dividends):")
    print(dividend_yield)
    visualize_data(dividend_yield, "Dividend Yield (%) by Stock", "ticker", "yield (%)", "Ticker", "Dividend Yield (%)", color="orange")

    insider_summary = summarize_insider_transactions(insider_transactions, stocks)
    print("\nInsider Transactions Summary:")
    print(insider_summary)
    visualize_data(insider_summary, "Net Insider Transactions by Stock", "ticker", "change", "Ticker", "Net Change in Shares", color="purple")

    news_sentiment = calculate_news_sentiment(news, stocks)
    print("\nNews Sentiment Analysis:")
    print(news_sentiment)
    visualize_data(news_sentiment, "Average News Sentiment by Stock", "ticker", "sentiment", "Ticker", "Average Sentiment", color="green")

    print("\nVisualizing News Topics:")
    visualize_news_topics(news, stocks)

    print("\nVisualizing Stock Prices Over Time:")
    visualize_stock_prices(stock_prices, stocks)


if __name__ == "__main__":
    main()
