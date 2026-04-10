import yfinance as yf
import pandas as pd
from datetime import datetime
import time
import logging
import requests

# ---------------- SETTINGS ----------------
stocks = ["AAPL", "TSLA", "INFY.NS", "RELIANCE.NS"]
MAX_RETRIES = 5
RETRY_DELAY = 300  # 5 minutes

# ---------------- LOGGING SETUP ----------------
logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("Pipeline started")
# ---------------- INTERNET CHECK ----------------
def check_internet():
    try:
        requests.get("https://www.google.com", timeout=5)
        return True
    except:
        return False

# ---------------- MAIN PIPELINE ----------------
attempt = 0

while attempt < MAX_RETRIES:
    try:
        print(f"Attempt {attempt+1}...")

        if not check_internet():
            raise Exception("No internet connection")

        all_data = pd.DataFrame()

        for stock_name in stocks:
            stock = yf.Ticker(stock_name)
            data = stock.history(period="5d")

            data['Stock'] = stock_name

            info = stock.info
            data['Company'] = info.get('longName', 'N/A')
            data['Sector'] = info.get('sector', 'N/A')
            data['MarketCap'] = info.get('marketCap', 'N/A')

            all_data = pd.concat([all_data, data])

        all_data.reset_index(inplace=True)

        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"stock_data_{today}.csv"

        all_data.to_csv(filename, index=False)

        print(f"Saved file: {filename}")
        logging.info(f"SUCCESS - File saved: {filename}")
        import psycopg2
        conn = psycopg2.connect(
            host="localhost",
            database="stock_db",
            user="postgres",
            password="6671"
        )
        cur = conn.cursor()
        for _, row in all_data.iterrows():
            cur.execute(""" INSERT INTO stock_data(date, open, high, low, close, volume, dividends, stock_splits, stock, company, sector, marketcap)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (
                row['Date'],
                row['Open'],
                row['High'],
                row['Low'],
                row['Close'],
                row['Volume'],
                row['Dividends'],
                row['Stock Splits'],
                row['Stock'],
                row['Company'],
                row['Sector'],
                row['MarketCap']
            ))
        conn.commit()
        cur.close() 
        conn.close()
        print("Data inserted into PostgreSQL!")

        break  # success → exit loop

    except Exception as e:
        attempt += 1
        logging.error(f"FAILED attempt {attempt}: {str(e)}")
        print(f"Error: {e}")

        if attempt < MAX_RETRIES:
            print("Retrying in 5 minutes...")
            time.sleep(RETRY_DELAY)
        else:
            print("Max retries reached. Exiting...")
            logging.critical("Pipeline failed after maximum retries.")