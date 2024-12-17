import sqlite3
import matplotlib.pyplot as plt
import pandas as pd

DB_NAME = "stock_data.db"
OUTPUT_TEXT_FILE = "stock_summary.txt"

def fetch_data(query):
    conn = sqlite3.connect(DB_NAME)
    data = pd.read_sql_query(query, conn)
    conn.close()
    return data

def calculate_summary():
    summary = []
    summary.append("=" * 50)
    summary.append("STOCK DATA ANALYSIS REPORT")
    summary.append("=" * 50)
    summary.append(f"Generated on: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")


    avg_close_query = """
        SELECT s.ticker, ROUND(AVG(sp.close), 2) AS avg_closing_price,
               MIN(sp.close) AS min_price, MAX(sp.close) AS max_price
        FROM StockPrices sp
        JOIN Stocks s ON sp.stock_id = s.stock_id
        GROUP BY s.ticker
    """
    avg_close_data = fetch_data(avg_close_query)
    summary.append("### Average Closing Prices ###")
    summary.extend([
        f" - {row['ticker']}: Avg=${row['avg_closing_price']}, Min=${row['min_price']}, Max=${row['max_price']}"
        for _, row in avg_close_data.iterrows()
    ])
    summary.append("")


    total_dividends_query = """
        SELECT s.ticker, IFNULL(ROUND(SUM(d.dividend) / 100.0, 2), 0) AS total_dividends
        FROM Stocks s
        LEFT JOIN Dividends d ON s.stock_id = d.stock_id
        GROUP BY s.ticker
    """
    total_dividends_data = fetch_data(total_dividends_query)
    summary.append("### Total Dividends per Stock ###")
    summary.extend([f" - {row['ticker']}: ${row['total_dividends']}" for _, row in total_dividends_data.iterrows()])
    summary.append("")

    total_transactions_query = """
        SELECT s.ticker, COUNT(it.transaction_id) AS total_transactions
        FROM InsiderTransactions it
        JOIN Stocks s ON it.stock_id = s.stock_id
        GROUP BY s.ticker
    """
    transactions_data = fetch_data(total_transactions_query)
    summary.append("### Total Insider Transactions per Stock ###")
    summary.extend([f" - {row['ticker']}: {row['total_transactions']} transactions" for _, row in transactions_data.iterrows()])
    summary.append("")

    avg_sentiment_query = """
        SELECT s.ticker, ROUND(AVG(sentiment), 2) AS avg_sentiment
        FROM Sentiments se
        JOIN Stocks s ON se.stock_id = s.stock_id
        GROUP BY s.ticker
    """
    avg_sentiment_data = fetch_data(avg_sentiment_query)
    summary.append("### Average Sentiment per Stock ###")
    summary.extend([f" - {row['ticker']}: Sentiment Score = {row['avg_sentiment']}" for _, row in avg_sentiment_data.iterrows()])
    summary.append("")

    summary.append("### Closing Price Trends Over Time ###")
    summary.append("This section shows the minimum and maximum prices, and trends for each stock.\n")
    trend_query = """
        SELECT s.ticker, sp.date, sp.close
        FROM StockPrices sp
        JOIN Stocks s ON sp.stock_id = s.stock_id
        ORDER BY s.ticker, sp.date
    """
    trend_data = fetch_data(trend_query)

    for ticker in trend_data['ticker'].unique():
        subset = trend_data[trend_data['ticker'] == ticker]
        min_price = subset['close'].min()
        max_price = subset['close'].max()
        min_date = subset.loc[subset['close'].idxmin(), 'date']
        max_date = subset.loc[subset['close'].idxmax(), 'date']

        summary.append(f" - {ticker}: Min=${min_price} on {min_date}, Max=${max_price} on {max_date}")
    summary.append("")

    summary.append("=" * 50)
    summary.append("End of Report")
    summary.append("=" * 50)
    with open(OUTPUT_TEXT_FILE, "w") as f:
        f.write("\n".join(summary))

    print(f"Formatted report written to {OUTPUT_TEXT_FILE}")
    return avg_close_data, total_dividends_data, transactions_data, avg_sentiment_data


def plot_avg_closing_prices(avg_close_data):
    """Visualize average closing prices."""
    plt.figure(figsize=(8, 5))
    plt.bar(avg_close_data['ticker'], avg_close_data['avg_closing_price'], color='skyblue')
    plt.title("Average Closing Prices per Stock")
    plt.xlabel("Stock Ticker")
    plt.ylabel("Average Closing Price ($)")
    plt.tight_layout()
    plt.savefig("average_closing_prices.png")
    plt.show()


def plot_total_dividends():
    """Visualize total dividends including stocks with 0 dividends."""
    query = """
        SELECT s.ticker, 
               IFNULL(ROUND(SUM(d.dividend) / 100.0, 2), 0) AS total_dividends
        FROM Stocks s
        LEFT JOIN Dividends d ON s.stock_id = d.stock_id
        GROUP BY s.ticker
    """
    total_dividends_data = fetch_data(query)

    plt.figure(figsize=(8, 5))
    plt.bar(total_dividends_data['ticker'], total_dividends_data['total_dividends'], color='lightgreen')
    plt.title("Total Dividends per Stock")
    plt.xlabel("Stock Ticker")
    plt.ylabel("Total Dividends ($)")
    plt.tight_layout()
    plt.savefig("total_dividends.png")
    plt.show()


def plot_total_transactions():
    """Visualize total insider transactions."""
    query = """
        SELECT s.ticker, COUNT(it.transaction_id) AS total_transactions
        FROM InsiderTransactions it
        JOIN Stocks s ON it.stock_id = s.stock_id
        GROUP BY s.ticker
    """
    transactions_data = fetch_data(query)

    plt.figure(figsize=(8, 5))
    plt.bar(transactions_data['ticker'], transactions_data['total_transactions'], color='salmon')
    plt.title("Total Insider Transactions per Stock")
    plt.xlabel("Stock Ticker")
    plt.ylabel("Number of Transactions")
    plt.tight_layout()
    plt.savefig("total_insider_transactions.png")
    plt.show()


def plot_avg_sentiment():
    """Visualize average sentiment per stock."""
    query = """
        SELECT s.ticker, ROUND(AVG(sentiment), 2) AS avg_sentiment
        FROM Sentiments se
        JOIN Stocks s ON se.stock_id = s.stock_id
        GROUP BY s.ticker
    """
    avg_sentiment_data = fetch_data(query)

    plt.figure(figsize=(8, 5))
    plt.bar(avg_sentiment_data['ticker'], avg_sentiment_data['avg_sentiment'], color='orange')
    plt.title("Average Sentiment per Stock")
    plt.xlabel("Stock Ticker")
    plt.ylabel("Sentiment Score")
    plt.tight_layout()
    plt.savefig("average_sentiment.png")
    plt.show()


def plot_closing_price_trend():
    """Visualize closing price trends over time with a 7-day moving average."""
    query = """
        SELECT s.ticker, sp.date, sp.close
        FROM StockPrices sp
        JOIN Stocks s ON sp.stock_id = s.stock_id
        ORDER BY s.ticker, sp.date
    """
    data = fetch_data(query)

    data['date'] = pd.to_datetime(data['date'], format='%Y%m%d')

    plt.figure(figsize=(10, 6))

    for ticker in data['ticker'].unique():
        subset = data[data['ticker'] == ticker]
        subset = subset.sort_values(by='date')
        subset['7_day_avg'] = subset['close'].rolling(window=7).mean()
        plt.plot(subset['date'], subset['close'], label=f"{ticker} - Closing Price", alpha=0.5)
        plt.plot(subset['date'], subset['7_day_avg'], label=f"{ticker} - 7-Day MA", linewidth=2)

    plt.title("Closing Price Trends Over Time with 7-Day Moving Average")
    plt.xlabel("Date")
    plt.ylabel("Closing Price ($)")
    plt.legend()
    plt.tight_layout()
    plt.savefig("closing_price_trends_with_avg.png")
    plt.show()


def main():
    print("Performing summary calculations and generating visualizations...")
    avg_close_data, _, _, _ = calculate_summary()

    print("Generating visualizations...")
    plot_avg_closing_prices(avg_close_data)
    plot_total_dividends()
    plot_total_transactions()
    plot_avg_sentiment()
    plot_closing_price_trend()

    print("All tasks completed successfully!")


if __name__ == "__main__":
    main()
